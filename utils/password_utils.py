"""
password_utils.py - Password generation and validation utilities.

Provides secure password generation, strength analysis, and validation
with support for multiple complexity policies.
"""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
import string
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, List, Optional, Tuple


class PasswordStrength(Enum):
    """Password strength classification."""
    VERY_WEAK = 1
    WEAK = 2
    FAIR = 3
    STRONG = 4
    VERY_STRONG = 5

    def __str__(self) -> str:
        return self.name.replace("_", " ").title()


@dataclass(frozen=True)
class PasswordPolicy:
    """
    Password policy configuration.

    Attributes:
        min_length: Minimum password length
        max_length: Maximum password length (0 = no limit)
        require_uppercase: Require at least one uppercase letter
        require_lowercase: Require at least one lowercase letter
        require_digits: Require at least one digit
        require_special: Require at least one special character
        allowed_special_chars: String of allowed special characters
        disallow_common: Disallow commonly used passwords
        disallow_patterns: List of regex patterns to disallow
        max_repeated_chars: Max consecutive repeated characters (0 = no limit)
        check_entropy: Whether to enforce minimum entropy
        min_entropy_bits: Minimum entropy in bits
    """

    min_length: int = 8
    max_length: int = 128
    require_uppercase: bool = True
    require_lowercase: bool = True
    require_digits: bool = True
    require_special: bool = False
    allowed_special_chars: str = "!@#$%^&*()_+-=[]{}|;:,.<>?"
    disallow_common: bool = True
    disallow_patterns: Tuple[str, ...] = ()
    max_repeated_chars: int = 3
    check_entropy: bool = True
    min_entropy_bits: float = 40.0

    @classmethod
    def strict(cls) -> PasswordPolicy:
        """Create a strict password policy for high-security applications."""
        return cls(
            min_length=12,
            max_length=64,
            require_uppercase=True,
            require_lowercase=True,
            require_digits=True,
            require_special=True,
            disallow_common=True,
            max_repeated_chars=2,
            check_entropy=True,
            min_entropy_bits=60.0,
        )

    @classmethod
    def standard(cls) -> PasswordPolicy:
        """Create a standard password policy for most applications."""
        return cls(
            min_length=8,
            max_length=64,
            require_uppercase=True,
            require_lowercase=True,
            require_digits=True,
            require_special=False,
            disallow_common=True,
            max_repeated_chars=3,
            check_entropy=True,
            min_entropy_bits=40.0,
        )

    @classmethod
    def relaxed(cls) -> PasswordPolicy:
        """Create a relaxed password policy with minimal requirements."""
        return cls(
            min_length=6,
            max_length=128,
            require_uppercase=False,
            require_lowercase=True,
            require_digits=False,
            require_special=False,
            disallow_common=False,
            max_repeated_chars=0,
            check_entropy=False,
            min_entropy_bits=0.0,
        )

    def get_charset_size(self) -> int:
        """Calculate the character set size based on policy requirements."""
        size = 0
        if self.require_lowercase:
            size += 26
        if self.require_uppercase:
            size += 26
        if self.require_digits:
            size += 10
        if self.require_special:
            size += len(self.allowed_special_chars)
        return max(size, 1)


# Common passwords to disallow (top 1000 condensed)
COMMON_PASSWORDS = frozenset([
    "password", "123456", "12345678", "qwerty", "abc123", "monkey", "1234567",
    "letmein", "trustno1", "dragon", "baseball", "iloveyou", "master", "sunshine",
    "ashley", "bailey", "shadow", "123123", "654321", "superman", "qazwsx",
    "michael", "football", "password1", "password123", "batman", "login",
    "admin", "welcome", "hello", "charlie", "donald", "password1234",
    "qwerty123", "password1", "1234567890", "passw0rd", "access", "flower",
    "whatever", "qwertyuiop", "computer", "corvette", "jennifer", "michelle",
    "jordan", "hunter", "amanda", "jessica", "thunder", "cheese", "summer",
    "harley", "pepper", "killer", "hockey", "ranger", "thomas", "test",
    "tigger", "robert", "austin", "merlin", "maggie", "diamond", "chicken",
    "golden", "victoria", "matrix", "mustang", "william", "coffee", "silver",
    "midnight", "george", "yankees", "chelsea", "lover", "apple", "sparky",
    "biteme", "richard", "matthew", "internet", "mike", "daniel", "starwars",
    "builder", "john", "elephant", "andrew", "martin", "pepper", "jackson",
    "london", "winter", "blahblah", "ginger", "angels", "camaro", "peanut",
    "maverick", "soccer", "mercedes", "spider", "creative", "asdfgh",
])


class PasswordGenerator:
    """
    Configurable password generator with policy enforcement.

    Example:
        >>> gen = PasswordGenerator(policy=PasswordPolicy.standard())
        >>> password = gen.generate()
        >>> print(f"Generated: {password}")
        Generated: Kd8#mP2!
    """

    def __init__(
        self,
        policy: Optional[PasswordPolicy] = None,
        entropy_source: Optional[Callable[[int], bytes]] = None,
    ) -> None:
        """
        Initialize the password generator.

        Args:
            policy: Password policy to enforce
            entropy_source: Custom entropy source (default: secrets.token_bytes)
        """
        self._policy = policy or PasswordPolicy.standard()
        self._entropy_source = entropy_source or secrets.token_bytes

    @property
    def policy(self) -> PasswordPolicy:
        """Current password policy."""
        return self._policy

    def _get_charset(self) -> str:
        """Build character set from policy."""
        chars = ""
        if self._policy.require_lowercase:
            chars += string.ascii_lowercase
        if self._policy.require_uppercase:
            chars += string.ascii_uppercase
        if self._policy.require_digits:
            chars += string.digits
        if self._policy.require_special:
            chars += self._policy.allowed_special_chars
        return chars or string.ascii_letters

    def _get_required_chars(self) -> Dict[str, str]:
        """Get one required character from each required class."""
        required: Dict[str, str] = {}
        charset = self._get_charset()

        if self._policy.require_lowercase:
            required["lower"] = string.ascii_lowercase
        if self._policy.require_uppercase:
            required["upper"] = string.ascii_uppercase
        if self._policy.require_digits:
            required["digit"] = string.digits
        if self._policy.require_special:
            required["special"] = self._policy.allowed_special_chars

        return required

    def _get_random_char(self, charset: str) -> str:
        """Get a cryptographically random character from charset."""
        if not charset:
            return ""
        idx = secrets.randbelow(len(charset))
        return charset[idx]

    def generate(self, length: Optional[int] = None) -> str:
        """
        Generate a password according to policy.

        Args:
            length: Specific length (uses policy default if None)

        Returns:
            Generated password string
        """
        length = length or self._policy.min_length

        # Ensure length is within policy bounds
        length = max(self._policy.min_length, min(length, self._policy.max_length or length))

        charset = self._get_charset()
        required = self._get_required_chars()

        # Build password ensuring all requirements are met
        password_chars: List[str] = []

        # First, add one character from each required class
        for chars in required.values():
            password_chars.append(self._get_random_char(chars))

        # Fill remaining length with random characters
        remaining = length - len(password_chars)
        for _ in range(remaining):
            password_chars.append(self._get_random_char(charset))

        # Shuffle to randomize positions of required characters
        # Use Fisher-Yates with secure random
        arr = list(password_chars)
        for i in range(len(arr) - 1, 0, -1):
            j = secrets.randbelow(i + 1)
            arr[i], arr[j] = arr[j], arr[i]

        password = "".join(arr)

        # Handle repeated characters if policy requires
        if self._policy.max_repeated_chars > 0:
            password = self._remove_repeated_chars(password)

        return password

    def _remove_repeated_chars(self, password: str) -> str:
        """Remove consecutive repeated characters exceeding policy limit."""
        if self._policy.max_repeated_chars <= 0:
            return password

        result = []
        count = 1

        for i, char in enumerate(password):
            if i > 0 and char == password[i - 1]:
                count += 1
            else:
                count = 1

            if count <= self._policy.max_repeated_chars:
                result.append(char)
            elif count > self._policy.max_repeated_chars:
                # Replace with a different character
                charset = self._get_charset().replace(char, "")
                if charset:
                    result.append(self._get_random_char(charset))
                    count = 1

        return "".join(result)

    def generate_passphrase(self, word_count: int = 4, separator: str = "-") -> str:
        """
        Generate a memorable passphrase from random words.

        Args:
            word_count: Number of words in passphrase
            separator: Separator between words
        """
        words = self._WORD_LIST
        selected = [secrets.choice(words) for _ in range(word_count)]
        return separator.join(selected)

    # EFF word list for passphrases (shortened subset)
    _WORD_LIST = [
        "able", "acid", "aged", "also", "area", "army", "away", "baby", "back", "ball",
        "band", "bank", "base", "bath", "bear", "beat", "been", "beer", "bell", "belt",
        "best", "bill", "bird", "bite", "blow", "blue", "boat", "body", "bomb", "bond",
        "bone", "book", "boom", "born", "boss", "both", "bowl", "bulk", "burn", "bush",
        "busy", "call", "calm", "came", "camp", "card", "care", "case", "cash", "cast",
        "cell", "chat", "chip", "city", "club", "coal", "coat", "code", "cold", "come",
        "cool", "cope", "copy", "core", "cost", "crew", "crop", "dark", "data", "date",
        "dead", "deal", "dear", "debt", "deep", "deny", "desk", "dial", "diet", "dirt",
        "disc", "disk", "does", "done", "door", "dose", "down", "draw", "drew", "drop",
        "drug", "dual", "duke", "dust", "duty", "each", "earn", "ease", "east", "easy",
        "edge", "else", "even", "ever", "evil", "exit", "face", "fact", "fail", "fair",
        "fall", "fame", "farm", "fast", "fate", "fear", "feed", "feel", "feet", "fell",
        "felt", "file", "fill", "film", "find", "fine", "fire", "firm", "fish", "five",
        "flat", "flow", "folk", "food", "foot", "ford", "form", "fort", "four", "free",
        "from", "fuel", "full", "fund", "gain", "game", "gang", "gate", "gave", "gear",
        "gene", "gift", "girl", "give", "glad", "goal", "goes", "gold", "golf", "gone",
        "good", "gray", "grew", "grey", "grow", "gulf", "hair", "half", "hall", "hand",
        "hang", "hard", "harm", "hate", "have", "head", "hear", "heat", "heel", "held",
        "hell", "help", "here", "hero", "hide", "high", "hill", "hint", "hire", "hold",
        "hole", "holy", "home", "hope", "host", "hour", "huge", "hung", "hunt", "hurt",
        "idea", "inch", "into", "iron", "item", "jack", "jane", "jazz", "jean", "jobs",
        "john", "join", "joke", "josh", "jump", "jury", "just", "keep", "kept", "kick",
        "kids", "kill", "kind", "king", "knee", "knew", "know", "lack", "lady", "laid",
        "lake", "land", "lane", "last", "late", "lead", "left", "less", "life", "lift",
        "like", "line", "link", "list", "live", "load", "loan", "lock", "logo", "long",
        "look", "lord", "lose", "loss", "lost", "love", "luck", "made", "mail", "main",
        "make", "male", "many", "mark", "mass", "matt", "meal", "mean", "meat", "meet",
        "menu", "mere", "mike", "mild", "mile", "milk", "mill", "mind", "mine", "miss",
        "mode", "mood", "moon", "more", "most", "move", "much", "must", "name", "navy",
        "near", "neck", "need", "news", "next", "nice", "nick", "nine", "node", "none",
        "nose", "note", "okay", "once", "only", "onto", "open", "oral", "over", "pace",
        "pack", "page", "paid", "pain", "pair", "palm", "park", "part", "pass", "past",
        "path", "paul", "peak", "pick", "pile", "pink", "pipe", "plan", "play", "plot",
        "plug", "plus", "poll", "pool", "poor", "pope", "port", "post", "pull", "pure",
        "push", "quit", "race", "rail", "rain", "rank", "rare", "rate", "read", "real",
        "rear", "rely", "rent", "rest", "rice", "rich", "ride", "ring", "rise", "risk",
        "road", "rock", "role", "roll", "rome", "roof", "room", "root", "rose", "rule",
        "rush", "ruth", "safe", "said", "sake", "sale", "salt", "same", "sand", "sang",
        "save", "seat", "seed", "seek", "seem", "seen", "self", "sell", "send", "sent",
        "sept", "ship", "shop", "shot", "show", "shut", "sick", "side", "sign", "silk",
        "site", "size", "skin", "slip", "slow", "snow", "soft", "soil", "sold", "sole",
        "some", "song", "soon", "sort", "soul", "spot", "star", "stay", "step", "stop",
        "such", "suit", "sure", "take", "tale", "talk", "tall", "tank", "tape", "task",
        "team", "tech", "tell", "tend", "term", "test", "text", "than", "that", "them",
        "then", "they", "thin", "this", "thus", "till", "time", "tiny", "told", "toll",
        "tone", "tony", "took", "tool", "tour", "town", "tree", "trip", "true", "tube",
        "turn", "twin", "type", "unit", "upon", "used", "user", "vary", "vast", "very",
        "vice", "view", "vote", "wade", "wait", "wake", "walk", "wall", "want", "ward",
        "warm", "wash", "wave", "ways", "weak", "wear", "week", "well", "went", "were",
        "west", "what", "when", "whom", "wide", "wife", "wild", "will", "wind", "wine",
        "wing", "wire", "wise", "wish", "with", "wood", "word", "wore", "work", "yard",
        "yeah", "year", "yoga", "your", "zero", "zone",
    ]


class PasswordValidator:
    """
    Password strength analyzer and validator.

    Provides detailed feedback on password strength and policy compliance.
    """

    def __init__(self, policy: Optional[PasswordPolicy] = None) -> None:
        """
        Initialize the password validator.

        Args:
            policy: Password policy to validate against
        """
        self._policy = policy or PasswordPolicy.standard()

    @property
    def policy(self) -> PasswordPolicy:
        """Current password policy."""
        return self._policy

    def validate(self, password: str) -> ValidationResult:
        """
        Validate a password against the policy.

        Args:
            password: Password to validate

        Returns:
            ValidationResult with details
        """
        errors: List[str] = []
        warnings: List[str] = []
        score = 0

        # Length checks
        if len(password) < self._policy.min_length:
            errors.append(f"Password too short (minimum {self._policy.min_length} characters)")
        elif len(password) >= self._policy.min_length + 4:
            score += 1

        if self._policy.max_length > 0 and len(password) > self._policy.max_length:
            errors.append(f"Password too long (maximum {self._policy.max_length} characters)")

        # Character class checks
        has_lower = bool(re.search(r"[a-z]", password))
        has_upper = bool(re.search(r"[A-Z]", password))
        has_digit = bool(re.search(r"\d", password))
        has_special = bool(re.search(f"[{re.escape(self._policy.allowed_special_chars)}]", password))

        if self._policy.require_lowercase and not has_lower:
            errors.append("Password must contain at least one lowercase letter")
        elif has_lower:
            score += 1

        if self._policy.require_uppercase and not has_upper:
            errors.append("Password must contain at least one uppercase letter")
        elif has_upper:
            score += 1

        if self._policy.require_digits and not has_digit:
            errors.append("Password must contain at least one digit")
        elif has_digit:
            score += 1

        if self._policy.require_special and not has_special:
            errors.append("Password must contain at least one special character")
        elif has_special:
            score += 1

        # Common password check
        if self._policy.disallow_common:
            if password.lower() in COMMON_PASSWORDS:
                errors.append("Password is too common")
                score = 0

        # Pattern checks
        for pattern in self._policy.disallow_patterns:
            if re.search(pattern, password):
                errors.append(f"Password matches disallowed pattern")
                break

        # Repeated character check
        if self._policy.max_repeated_chars > 0:
            for i in range(len(password) - self._policy.max_repeated_chars):
                chunk = password[i : i + self._policy.max_repeated_chars + 1]
                if len(set(chunk)) == 1:
                    errors.append(f"Too many repeated characters")
                    break

        # Entropy check
        if self._policy.check_entropy:
            entropy = self._calculate_entropy(password)
            if entropy < self._policy.min_entropy_bits:
                errors.append(f"Password entropy too low ({entropy:.1f} bits, minimum {self._policy.min_entropy_bits:.1f} bits)")
            else:
                score += 2

        # Calculate strength
        strength = self._calculate_strength(password)
        if strength != PasswordStrength.VERY_WEAK and errors:
            # Downgrade strength if there are errors
            pass  # errors already contain the issues

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            strength=strength,
            score=score,
            entropy=self._calculate_entropy(password),
        )

    def _calculate_entropy(self, password: str) -> float:
        """Calculate Shannon entropy of password."""
        if not password:
            return 0.0

        import math

        freq: Dict[str, float] = {}
        for char in password:
            freq[char] = freq.get(char, 0) + 1

        entropy = 0.0
        for count in freq.values():
            p = count / len(password)
            if p > 0:
                entropy -= p * math.log2(p)

        # Multiply by length for total entropy
        return entropy * len(password)

    def _calculate_strength(self, password: str) -> PasswordStrength:
        """Calculate password strength classification."""
        entropy = self._calculate_entropy(password)

        if entropy < 28:
            return PasswordStrength.VERY_WEAK
        elif entropy < 36:
            return PasswordStrength.WEAK
        elif entropy < 60:
            return PasswordStrength.FAIR
        elif entropy < 80:
            return PasswordStrength.STRONG
        else:
            return PasswordStrength.VERY_STRONG

    def check_breached(self, password: str) -> bool:
        """
        Check if password appears in known breaches (using SHA1 prefix).

        Uses the HaveIBeenPwned k-anonymity API.
        """
        import urllib.request

        # Hash the password
        sha1 = hashlib.sha1(password.encode()).hexdigest().upper()
        prefix, suffix = sha1[:5], sha1[5:]

        try:
            url = f"https://api.pwnedpasswords.com/range/{prefix}"
            with urllib.request.urlopen(url, timeout=5) as response:
                data = response.read().decode()

            for line in data.split("\n"):
                if suffix in line:
                    return True
            return False
        except Exception:
            # Fail open - don't block on API errors
            return False


@dataclass
class ValidationResult:
    """Result of password validation."""

    is_valid: bool
    errors: List[str]
    warnings: List[str]
    strength: PasswordStrength
    score: int
    entropy: float

    def __str__(self) -> str:
        if self.is_valid:
            return f"Valid (Strength: {self.strength}, Score: {self.score}/{self.score + len(self.errors)}, Entropy: {self.entropy:.1f} bits)"
        else:
            return f"Invalid: {'; '.join(self.errors)}"


def hash_password(password: str, salt: Optional[bytes] = None, pepper: str = "") -> Tuple[str, str]:
    """
    Hash a password using PBKDF2 with SHA-256.

    Args:
        password: Plain text password
        salt: Salt bytes (generated if None)
        pepper: Secret pepper string

    Returns:
        Tuple of (hash_hex, salt_hex)
    """
    if salt is None:
        salt = secrets.token_bytes(32)
    if pepper:
        password = pepper + password

    key = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100000)
    return key.hex(), salt.hex()


def verify_password(
    password: str,
    hashed: str,
    salt: str,
    pepper: str = "",
) -> bool:
    """
    Verify a password against a stored hash.

    Args:
        password: Plain text password to verify
        hashed: Stored hash hex string
        salt: Stored salt hex string
        pepper: Secret pepper string

    Returns:
        True if password matches
    """
    salt_bytes = bytes.fromhex(salt)
    if pepper:
        password = pepper + password

    key = hashlib.pbkdf2_hmac("sha256", password.encode(), salt_bytes, 100000)
    return hmac.compare_digest(key.hex(), hashed)


# Convenience functions
def generate_secure_password(
    length: int = 16,
    strict: bool = False,
) -> str:
    """
    Generate a secure random password.

    Args:
        length: Password length
        strict: Use strict policy

    Returns:
        Generated password
    """
    policy = PasswordPolicy.strict() if strict else PasswordPolicy.standard()
    return PasswordGenerator(policy=policy).generate(length=length)


def check_password_strength(password: str) -> ValidationResult:
    """
    Check password strength.

    Args:
        password: Password to check

    Returns:
        ValidationResult with strength analysis
    """
    return PasswordValidator().validate(password)
