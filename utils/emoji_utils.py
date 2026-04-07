"""
Emoji utilities for detection, conversion, and manipulation.

Provides:
- Emoji detection and validation
- Emoji to text description conversion
- Emoji extraction and removal
- Category-based emoji filtering
- Emoji skin tone modifiers
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Iterator, Optional


# Comprehensive emoji patterns
EMOJI_PATTERN = re.compile(
    "["
    "\U0001F600-\U0001F64F"  # emoticons
    "\U0001F300-\U0001F5FF"  # symbols & pictographs
    "\U0001F680-\U0001F6FF"  # transport & map symbols
    "\U0001F1E0-\U0001F1FF"  # flags
    "\U0001F700-\U0001F77F"  # alchemical symbols
    "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
    "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
    "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
    "\U0001FA00-\U0001FA6F"  # Chess Symbols
    "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
    "\U00002702-\U000027B0"  # Dingbats
    "\U000024C2-\U0001F251"  # enclosed characters
    "\U00002600-\U000026FF"  # misc symbols
    "\U00002700-\U000027BF"  # dingbats
    "\U0001F900-\U0001F9FF"  # supplemental symbols
    "\U0001FA70-\U0001FAFF"  # extended-a
    "\U0001FAB0-\U0001FABF"  # extended
    "\U0001FAC0-\U0001FACF"  # extended
    "\U0001FAD0-\U0001FADF"  # extended
    "]+",
    flags=re.UNICODE,
)

EMOJI_SHORTCODE_PATTERN = re.compile(r":([a-zA-Z0-9_+-]+):")


class EmojiCategory(Enum):
    """Emoji categories."""

    SMILEY = "Smiley & Emotion"
    PEOPLE = "People & Body"
    ANIMAL = "Animals & Nature"
    FOOD = "Food & Drink"
    ACTIVITY = "Activities"
    TRAVEL = "Travel & Places"
    OBJECTS = "Objects"
    SYMBOLS = "Symbols"
    FLAGS = "Flags"


# Emoji to text descriptions mapping
EMOJI_DESCRIPTIONS: dict[str, str] = {
    "😀": "grinning face",
    "😃": "grinning face with big eyes",
    "😄": "grinning face with smiling eyes",
    "😁": "beaming face with smiling eyes",
    "😊": "smiling face with smiling eyes",
    "🙂": "slightly smiling face",
    "🙃": "upside-down face",
    "😉": "winking face",
    "😍": "smiling face with heart-eyes",
    "🥰": "smiling face with hearts",
    "😎": "smiling face with sunglasses",
    "🤩": "star-struck",
    "😢": "crying face",
    "😭": "loudly crying face",
    "😱": "face screaming in fear",
    "😡": "pouting face",
    "😤": "face with steam from nose",
    "🤔": "thinking face",
    "🤫": "shushing face",
    "🤐": "zipper-mouth face",
    "😶": "face without mouth",
    "😴": "sleeping face",
    "🤧": "sneezing face",
    "😷": "face with medical mask",
    "🤒": "face with thermometer",
    "👍": "thumbs up",
    "👎": "thumbs down",
    "👏": "clapping hands",
    "🙌": "raising hands",
    "🤝": "handshake",
    "🙏": "folded hands",
    "✌️": "victory hand",
    "🤞": "crossed fingers",
    "☝️": "index pointing up",
    "👆": "index pointing up",
    "👇": "index pointing down",
    "👉": "index pointing right",
    "👈": "index pointing left",
    "💪": "flexed biceps",
    "🦾": "mechanical arm",
    "🦿": "mechanical leg",
    "❤️": "red heart",
    "🧡": "orange heart",
    "💛": "yellow heart",
    "💚": "green heart",
    "💙": "blue heart",
    "💜": "purple heart",
    "🖤": "black heart",
    "🤍": "white heart",
    "💔": "broken heart",
    "❣️": "heart exclamation",
    "💕": "two hearts",
    "💖": "glowing heart",
    "💗": "growing heart",
    "💘": "heart with arrow",
    "💝": "heart with ribbon",
    "⭐": "star",
    "🌟": "glowing star",
    "✨": "sparkles",
    "💫": "dizzy star",
    "⚡": "high voltage",
    "🔥": "fire",
    "💥": "collision",
    "💢": "anger symbol",
    "💦": "sweat droplets",
    "💨": "dashing away",
    "💣": "bomb",
    "💬": "speech balloon",
    "👁️": "eye",
    "👀": "eyes",
    "👅": "tongue",
    "👄": "mouth",
    "👋": "waving hand",
    "🤚": "raised hand",
    "✋": "raised hand",
    "🖐️": "hand with fingers splayed",
    "🖖": "vulcan salute",
    "👓": "glasses",
    "🥽": "goggles",
    "🎧": "headphone",
    "🎵": "musical note",
    "🎶": "musical notes",
    "🎤": "microphone",
    "🎧": "headphone",
    "🔔": "bell",
    "🔕": "bell with slash",
    "🔍": "magnifying glass left",
    "🔎": "magnifying glass right",
    "🔐": "locked with key",
    "🔏": "locked with pen",
    "🔒": "locked",
    "🔓": "unlocked",
    "🔑": "key",
    "🔨": "hammer",
    "⛏️": "pick",
    "🔧": "wrench",
    "🔩": "nut and bolt",
    "⚙️": "gear",
    "🔗": "link",
    "📎": "paperclip",
    "📌": "pin",
    "✂️": "scissors",
    "🔪": "kitchen knife",
    "🗡️": "dagger",
    "⚔️": "crossed swords",
    "🔫": "water pistol",
    "💊": "pill",
    "💉": "syringe",
    "🩸": "drop of blood",
    "🏥": "hospital",
    "🚑": "ambulance",
    "🚒": "fire engine",
    "🚓": "police car",
    "🚨": "police car light",
    "🚲": "bicycle",
    "🚗": "car",
    "🚕": "taxi",
    "🚌": "bus",
    "🚎": "trolleybus",
    "🏎️": "racing car",
    "🚓": "police car",
    "✈️": "airplane",
    "🚀": "rocket",
    "🛸": "flying saucer",
    "🚁": "helicopter",
    "⛵": "sailboat",
    "🚤": "speedboat",
    "🛳️": "passenger ship",
    "⛴️": "ferry",
    "🚢": "ship",
    "⚓": "anchor",
    "🏖️": "beach with umbrella",
    "🏔️": "snow-capped mountain",
    "🌋": "volcano",
    "🗻": "mount fuji",
    "🏕️": "camping",
    "🏗️": "building construction",
    "🏠": "house",
    "🏡": "house with garden",
    "🏢": "office building",
    "🏣": "Japanese post office",
    "🏥": "hospital",
    "🏦": "bank",
    "🏨": "hotel",
    "🏩": "love hotel",
    "🏪": "convenience store",
    "🏫": "school",
    "💒": "wedding",
    "🏛️": "classical building",
    "⛪": "church",
    "🕌": "mosque",
    "🕍": "synagogue",
    "⛩️": "shinto shrine",
    "🕋": "kaaba",
    "🌍": "globe showing Europe-Africa",
    "🌎": "globe showing Americas",
    "🌏": "globe showing Asia-Australia",
    "🌐": "globe with meridians",
    "🪐": "ringed planet",
    "🌞": "sun with face",
    "🌝": "full moon with face",
    "🌚": "new moon with face",
    "🌛": "first quarter moon with face",
    "🌜": "last quarter moon with face",
    "🌙": "crescent moon",
    "🌕": "full moon",
    "🌖": "waning gibbous moon",
    "🌗": "last quarter moon",
    "🌘": "waning crescent moon",
    "🌑": "new moon",
    "🌒": "waxing crescent moon",
    "🌓": "first quarter moon",
    "🌔": "waxing gibbous moon",
    "🌈": "rainbow",
    "☀️": "sun",
    "🌤️": "sun behind small cloud",
    "⛅": "sun behind cloud",
    "🌥️": "sun behind large cloud",
    "☁️": "cloud",
    "🌧️": "cloud with rain",
    "⛈️": "cloud with lightning and rain",
    "🌩️": "cloud with lightning",
    "❄️": "snowflake",
    "☃️": "snowman",
    "⛄": "snowman without snow",
    "🌬️": "wind face",
    "💨": "dashing wind",
    "🌪️": "tornado",
    "🌫️": "fog",
    "🌊": "water wave",
    "🌫️": "foggy",
    "🎈": "balloon",
    "🎉": "party popper",
    "🎊": "confetti ball",
    "🎄": "Christmas tree",
    "🎃": "jack-o-lantern",
    "�🎃": "halloween",
    "🎅": "Santa Claus",
    "🤶": "Mrs. Claus",
    "🦌": "deer",
    "🐶": "dog face",
    "🐱": "cat face",
    "🐭": "mouse face",
    "🐹": "hamster",
    "🐰": "rabbit face",
    "🦊": "fox",
    "🐻": "bear",
    "🐼": "panda",
    "🐨": "koala",
    "🐯": "tiger face",
    "🦁": "lion",
    "🐮": "cow face",
    "🐷": "pig face",
    "🐸": "frog face",
    "🐵": "monkey face",
    "🙈": "see-no-evil monkey",
    "🙉": "hear-no-evil monkey",
    "🙊": "speak-no-evil monkey",
    "🐔": "chicken",
    "🐧": "penguin",
    "🐦": "bird",
    "🐤": "baby chick",
    "🦆": "duck",
    "🦅": "eagle",
    "🦉": "owl",
    "🦇": "bat",
    "🐺": "wolf",
    "🐗": "boar",
    "🐴": "horse face",
    "🦄": "unicorn",
    "🐝": "honeybee",
    "🐛": "bug",
    "🦋": "butterfly",
    "🐌": "snail",
    "🐞": "lady beetle",
    "🐜": "ant",
    "🦟": "mosquito",
    "🦗": "cricket",
    "🕷️": "spider",
    "🦂": "scorpion",
    "🐢": "turtle",
    "🐍": "snake",
    "🦎": "lizard",
    "🦖": "T-Rex",
    "🦕": "sauropod",
    "🐙": "octopus",
    "🦑": "squid",
    "🦐": "shrimp",
    "🦀": "crab",
    "🐠": "tropical fish",
    "🐟": "fish",
    "🐬": "dolphin",
    "🐳": "spouting whale",
    "🐋": "whale",
    "🦈": "shark",
    "🐊": "crocodile",
    "🐅": "tiger",
    "🦓": "zebra",
    "🦍": "gorilla",
    "🦧": "orangutan",
    "🐘": "elephant",
    "🦛": "hippopotamus",
    "🦏": "rhinoceros",
}


def find_emojis(text: str) -> list[str]:
    """
    Find all emojis in text.

    Args:
        text: Input text

    Returns:
        List of emoji characters

    Example:
        >>> find_emojis("Hello 👋 World 🌍!")
        ['👋', '🌍']
    """
    return EMOJI_PATTERN.findall(text)


def count_emojis(text: str) -> int:
    """Count the number of emojis in text."""
    return len(find_emojis(text))


def remove_emojis(text: str) -> str:
    """
    Remove all emojis from text.

    Args:
        text: Input text

    Returns:
        Text without emojis

    Example:
        >>> remove_emojis("Hello 👋 World 🌍!")
        'Hello  World !'
    """
    return EMOJI_PATTERN.sub("", text)


def replace_emojis(text: str, replacement: str = "") -> str:
    """
    Replace all emojis with a string.

    Args:
        text: Input text
        replacement: Replacement string

    Returns:
        Text with emojis replaced
    """
    return EMOJI_PATTERN.sub(replacement, text)


def extract_emoji_info(text: str) -> list[dict[str, Any]]:
    """
    Extract emojis with their positions.

    Args:
        text: Input text

    Returns:
        List of dicts with emoji, start, end positions
    """
    results: list[dict[str, Any]] = []
    for match in EMOJI_PATTERN.finditer(text):
        results.append({"emoji": match.group(), "start": match.start(), "end": match.end()})
    return results


def emoji_to_description(emoji: str) -> str:
    """
    Get text description of an emoji.

    Args:
        emoji: Single emoji character

    Returns:
        Human-readable description

    Example:
        >>> emoji_to_description("❤️")
        'red heart'
    """
    return EMOJI_DESCRIPTIONS.get(emoji, "emoji")


def describe_text_emojis(text: str) -> list[tuple[str, str]]:
    """
    Get descriptions for all emojis in text.

    Args:
        text: Input text

    Returns:
        List of (emoji, description) tuples
    """
    return [(emoji, emoji_to_description(emoji)) for emoji in find_emojis(text)]


def contains_emoji(text: str) -> bool:
    """Check if text contains any emoji."""
    return EMOJI_PATTERN.search(text) is not None


def is_single_emoji(text: str) -> bool:
    """Check if text is exactly one emoji."""
    stripped = text.strip()
    return stripped != "" and EMOJI_PATTERN.fullmatch(stripped) is not None


def emoji_frequency(text: str) -> dict[str, int]:
    """
    Count emoji frequency in text.

    Args:
        text: Input text

    Returns:
        Dictionary of emoji -> count
    """
    emojis = find_emojis(text)
    return {emoji: emojis.count(emoji) for emoji in sorted(set(emojis))}


def demojize(text: str, delimiters: tuple[str, str] = (":", ":")) -> str:
    """
    Convert emojis to shortcode names.

    Args:
        text: Input text
        delimiters: Opening and closing delimiters

    Returns:
        Text with emojis as :name: format

    Example:
        >>> demojize("Hello 👋")
        'Hello :waving_hand:'
    """
    def replacer(match: re.Match) -> str:
        emoji = match.group()
        desc = emoji_to_description(emoji).replace(" ", "_").lower()
        return f"{delimiters[0]}{desc}{delimiters[1]}"

    return EMOJI_PATTERN.sub(replacer, text)


def emojize(text: str) -> str:
    """
    Convert shortcodes back to emojis.

    Args:
        text: Text with :name: shortcodes

    Returns:
        Text with emojis

    Example:
        >>> emojize("Hello :waving_hand:")
        'Hello 👋'
    """
    shortcode_map = {v.replace(" ", "_").lower(): k for k, v in EMOJI_DESCRIPTIONS.items()}

    def replacer(match: re.Match) -> str:
        name = match.group(1).lower()
        return shortcode_map.get(name, match.group(0))

    return EMOJI_SHORTCODE_PATTERN.sub(replacer, text)


def strip_emoji_modifiers(text: str) -> str:
    """
    Remove skin tone and other modifiers from emojis.

    Args:
        text: Input text

    Returns:
        Text with modifiers removed
    """
    # Remove skin tone modifiers (U+1F3FB - U+1F3FF)
    text = re.sub(r"[\U0001F3FB-\U0001F3FF]", "", text)
    # Remove hair style modifiers (U+1F9B0 - U+1F9B3)
    text = re.sub(r"[\U0001F9B0-\U0001F9B3]", "", text)
    return text


def get_emoji_categories(emojis: list[str]) -> list[EmojiCategory]:
    """
    Categorize a list of emojis.

    Returns:
        List of categories for the emojis
    """
    # This is a simplified version - full implementation would need a comprehensive mapping
    return []


def normalize_emoji(text: str) -> str:
    """
    Normalize emoji to their canonical form.

    Args:
        text: Input text

    Returns:
        Normalized text
    """
    import unicodedata

    return unicodedata.normalize("NFKC", text)


@dataclass
class EmojiStats:
    """Statistics about emoji usage."""

    total_emojis: int
    unique_emojis: int
    most_common: list[tuple[str, int]]
    categories: dict[EmojiCategory, int]


def analyze_emoji_usage(text: str) -> EmojiStats:
    """
    Analyze emoji usage in text.

    Args:
        text: Input text

    Returns:
        EmojiStats with usage statistics
    """
    emojis = find_emojis(text)
    freq = emoji_frequency(text)

    return EmojiStats(
        total_emojis=len(emojis),
        unique_emojis=len(freq),
        most_common=sorted(freq.items(), key=lambda x: x[1], reverse=True)[:10],
        categories={},
    )


from typing import Any
