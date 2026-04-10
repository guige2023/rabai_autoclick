"""
语音控制系统 v22
P0级差异化功能 - 免手动操作的工作流控制

功能:
- 语音命令识别和执行
- 唤醒词检测
- 语音启动工作流
- 语音反馈
- 语音听写
- 自然语言控制
- 语音活动检测(VAD)
- 多语言支持
- 语音训练
- 语音快捷键
"""

import json
import time
import re
import os
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import logging
import threading
import struct
import wave
import math

# ============== Enums ==============

class VoiceControlState(Enum):
    """语音控制状态"""
    IDLE = "idle"
    LISTENING = "listening"
    PROCESSING = "processing"
    SPEAKING = "speaking"
    ERROR = "error"


class VoiceActivity(Enum):
    """语音活动状态"""
    SILENCE = "silence"
    SPEECH = "speech"
    UNKNOWN = "unknown"


class Language(Enum):
    """支持的语言"""
    ENGLISH = "en"
    CHINESE = "zh"
    SPANISH = "es"
    FRENCH = "fr"
    GERMAN = "de"
    JAPANESE = "ja"
    KOREAN = "ko"


# ============== Data Classes ==============

@dataclass
class VoiceCommand:
    """语音命令"""
    command_id: str
    text: str
    language: Language
    confidence: float
    timestamp: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class VoiceShortcut:
    """语音快捷键"""
    shortcut_id: str
    phrase: str
    action: str
    params: Dict[str, Any] = field(default_factory=dict)
    language: Language = Language.ENGLISH
    usage_count: int = 0


@dataclass
class VoiceTrainingProfile:
    """语音训练档案"""
    user_id: str
    language: Language
    acoustic_model_path: Optional[str] = None
    language_model_path: Optional[str] = None
    adaptation_data: Dict[str, Any] = field(default_factory=dict)
    last_updated: float = 0


@dataclass
class WorkflowVoiceLaunch:
    """语音启动的工作流"""
    workflow_id: str
    workflow_name: str
    voice_command: str
    auto_confirm: bool = False


@dataclass
class VoiceFeedback:
    """语音反馈配置"""
    enabled: bool = True
    volume: float = 0.8
    rate: float = 1.0
    pitch: float = 1.0
    voice_id: Optional[str] = None


# ============== Voice Control Class =============

class VoiceControl:
    """
    语音控制系统
    提供免手动操作的工作流控制能力
    """

    def __init__(
        self,
        wake_word: str = "hey assistant",
        language: Language = Language.ENGLISH,
        enable_vad: bool = True,
        enable_voice_feedback: bool = True,
        tts_enabled: bool = True,
        asr_enabled: bool = True,
    ):
        """
        初始化语音控制系统

        Args:
            wake_word: 唤醒词
            language: 默认语言
            enable_vad: 启用语音活动检测
            enable_voice_feedback: 启用语音反馈
            tts_enabled: 启用语音合成
            asr_enabled: 启用语音识别
        """
        self.wake_word = wake_word.lower()
        self.language = language
        self.enable_vad = enable_vad
        self.enable_voice_feedback = enable_voice_feedback
        self.tts_enabled = tts_enabled
        self.asr_enabled = asr_enabled

        # 状态管理
        self.state = VoiceControlState.IDLE
        self.is_active = False
        self.is_listening = False

        # 语音活动检测
        self.vad_threshold = 0.5
        self.silence_timeout = 2.0
        self.speech_timeout = 30.0
        self.current_activity = VoiceActivity.UNKNOWN

        # 音频配置
        self.sample_rate = 16000
        self.channels = 1
        self.chunk_size = 1024

        # 语音命令注册表
        self.commands: Dict[str, Callable] = {}
        self.command_patterns: List[Dict[str, Any]] = []

        # 语音快捷键
        self.shortcuts: Dict[str, VoiceShortcut] = {}
        self._initialize_default_shortcuts()

        # 语音训练档案
        self.training_profiles: Dict[str, VoiceTrainingProfile] = {}
        self.current_profile: Optional[VoiceTrainingProfile] = None

        # 工作流语音启动映射
        self.workflow_launches: Dict[str, WorkflowVoiceLaunch] = {}

        # 语音反馈
        self.feedback = VoiceFeedback(enabled=enable_voice_feedback)

        # 自然语言理解映射
        self.nl_commands: Dict[str, List[str]] = defaultdict(list)
        self._initialize_nl_mappings()

        # 回调函数
        self.on_wake_word_detected: Optional[Callable] = None
        self.on_command_recognized: Optional[Callable] = None
        self.on_workflow_launch: Optional[Callable] = None
        self.on_voice_activity: Optional[Callable] = None
        self.on_error: Optional[Callable] = None

        # 日志
        self.logger = logging.getLogger(__name__)

        # 多语言支持
        self.language_models: Dict[Language, Dict[str, Any]] = {}
        self._initialize_language_models()

        # 命令历史
        self.command_history: List[VoiceCommand] = []
        self.max_history_size = 1000

    def _initialize_default_shortcuts(self) -> None:
        """初始化默认语音快捷键"""
        default_shortcuts = [
            VoiceShortcut(
                shortcut_id="start_workflow",
                phrase="start workflow",
                action="launch_workflow",
                language=Language.ENGLISH
            ),
            VoiceShortcut(
                shortcut_id="stop_workflow",
                phrase="stop",
                action="stop_workflow",
                language=Language.ENGLISH
            ),
            VoiceShortcut(
                shortcut_id="pause_workflow",
                phrase="pause",
                action="pause_workflow",
                language=Language.ENGLISH
            ),
            VoiceShortcut(
                shortcut_id="resume_workflow",
                phrase="resume",
                action="resume_workflow",
                language=Language.ENGLISH
            ),
            VoiceShortcut(
                shortcut_id="show_status",
                phrase="what's running",
                action="show_status",
                language=Language.ENGLISH
            ),
            VoiceShortcut(
                shortcut_id="help",
                phrase="help me",
                action="show_help",
                language=Language.ENGLISH
            ),
        ]
        for shortcut in default_shortcuts:
            self.shortcuts[shortcut.shortcut_id] = shortcut

    def _initialize_nl_mappings(self) -> None:
        """初始化自然语言命令映射"""
        # 启动类命令
        self.nl_commands["start"] = [
            "start", "launch", "begin", "run", "execute",
            "开始", "启动", "运行", "执行"
        ]
        # 停止类命令
        self.nl_commands["stop"] = [
            "stop", "halt", "terminate", "end", "cancel",
            "停止", "终止", "结束", "取消"
        ]
        # 暂停类命令
        self.nl_commands["pause"] = [
            "pause", "wait", "hold", "suspend",
            "暂停", "等待", "挂起"
        ]
        # 继续类命令
        self.nl_commands["resume"] = [
            "resume", "continue", "proceed", "go on",
            "继续", "恢复"
        ]
        # 状态查询
        self.nl_commands["status"] = [
            "status", "what's running", "show status", "check",
            "状态", "运行情况", "检查"
        ]
        # 帮助
        self.nl_commands["help"] = [
            "help", "help me", "what can you do", "commands",
            "帮助", "帮忙", "你能做什么"
        ]

    def _initialize_language_models(self) -> None:
        """初始化语言模型配置"""
        for lang in Language:
            self.language_models[lang] = {
                "name": lang.value,
                "commands": {},
                "wake_words": [],
                "phonetic_model": None
            }

    # ============== Wake Word Detection ==============

    def listen_for_wake_word(self, audio_stream: Any = None) -> bool:
        """
        监听唤醒词

        Args:
            audio_stream: 音频流（如果为None则使用模拟数据）

        Returns:
            是否检测到唤醒词
        """
        if not self.asr_enabled:
            return False

        self.state = VoiceControlState.LISTENING
        self.logger.info(f"Listening for wake word: {self.wake_word}")

        # 模拟唤醒词检测（实际实现需要音频处理库）
        if audio_stream is None:
            # 模拟检测
            time.sleep(0.1)
            detected = True
        else:
            # 实际音频处理
            detected = self._process_audio_for_wake_word(audio_stream)

        if detected:
            self.logger.info("Wake word detected!")
            self.is_active = True
            self.state = VoiceControlState.IDLE

            if self.on_wake_word_detected:
                self.on_wake_word_detected(self.wake_word)

            if self.enable_voice_feedback:
                self.speak("Yes?")

        return detected

    def _process_audio_for_wake_word(self, audio_stream: Any) -> bool:
        """
        处理音频流检测唤醒词

        Args:
            audio_stream: 音频流

        Returns:
            是否检测到唤醒词
        """
        # 实际实现需要:
        # 1. 读取音频数据
        # 2. 进行特征提取
        # 3. 使用唤醒词模型进行检测
        # 这里使用简化的模拟实现
        try:
            # 模拟音频处理
            return True
        except Exception as e:
            self.logger.error(f"Wake word detection error: {e}")
            return False

    def set_wake_word(self, wake_word: str) -> None:
        """
        设置唤醒词

        Args:
            wake_word: 新的唤醒词
        """
        self.wake_word = wake_word.lower()
        self.logger.info(f"Wake word updated to: {self.wake_word}")

    # ============== Voice Activity Detection ==============

    def detect_voice_activity(self, audio_data: bytes) -> VoiceActivity:
        """
        检测语音活动

        Args:
            audio_data: 音频数据

        Returns:
            语音活动状态
        """
        if not self.enable_vad:
            return VoiceActivity.UNKNOWN

        try:
            # 计算音频能量
            energy = self._calculate_audio_energy(audio_data)

            if energy > self.vad_threshold:
                self.current_activity = VoiceActivity.SPEECH
            else:
                self.current_activity = VoiceActivity.SILENCE

            if self.on_voice_activity:
                self.on_voice_activity(self.current_activity, energy)

            return self.current_activity

        except Exception as e:
            self.logger.error(f"VAD error: {e}")
            return VoiceActivity.UNKNOWN

    def _calculate_audio_energy(self, audio_data: bytes) -> float:
        """
        计算音频能量

        Args:
            audio_data: 原始音频数据

        Returns:
            能量值 (0.0 - 1.0)
        """
        if not audio_data:
            return 0.0

        try:
            # 将字节数据转换为数值
            audio_samples = struct.unpack(f"{len(audio_data)//2}h", audio_data)
            # 计算RMS
            rms = math.sqrt(sum(s*s for s in audio_samples) / len(audio_samples))
            # 归一化到0-1
            return min(1.0, rms / 32768.0)
        except Exception:
            return 0.0

    def set_vad_parameters(
        self,
        threshold: float = None,
        silence_timeout: float = None,
        speech_timeout: float = None
    ) -> None:
        """
        设置VAD参数

        Args:
            threshold: 能量阈值 (0.0 - 1.0)
            silence_timeout: 静音超时时间(秒)
            speech_timeout: 语音超时时间(秒)
        """
        if threshold is not None:
            self.vad_threshold = max(0.0, min(1.0, threshold))
        if silence_timeout is not None:
            self.silence_timeout = silence_timeout
        if speech_timeout is not None:
            self.speech_timeout = speech_timeout

    # ============== Speech Recognition ==============

    def recognize_speech(
        self,
        audio_data: bytes,
        language: Language = None,
        timeout: float = 30.0
    ) -> Optional[VoiceCommand]:
        """
        识别语音

        Args:
            audio_data: 音频数据
            language: 语言（如果为None则使用默认语言）
            timeout: 超时时间

        Returns:
            语音命令或None
        """
        if not self.asr_enabled:
            return None

        if language is None:
            language = self.language

        self.state = VoiceControlState.PROCESSING
        start_time = time.time()

        try:
            # 检测语音活动
            activity = self.detect_voice_activity(audio_data)
            if activity == VoiceActivity.SILENCE:
                self.logger.debug("Silence detected, ignoring")
                return None

            # 模拟语音识别（实际实现需要ASR库如SpeechRecognition, Whisper等）
            text = self._simulate_speech_recognition(audio_data, language)

            if not text:
                return None

            # 计算置信度
            confidence = self._calculate_confidence(text)

            # 创建命令
            command = VoiceCommand(
                command_id=self._generate_command_id(),
                text=text,
                language=language,
                confidence=confidence,
                timestamp=time.time()
            )

            # 添加到历史
            self._add_to_history(command)

            self.state = VoiceControlState.IDLE

            if self.on_command_recognized:
                self.on_command_recognized(command)

            return command

        except Exception as e:
            self.logger.error(f"Speech recognition error: {e}")
            self.state = VoiceControlState.ERROR
            if self.on_error:
                self.on_error(str(e))
            return None

    def _simulate_speech_recognition(
        self,
        audio_data: bytes,
        language: Language
    ) -> str:
        """
        模拟语音识别

        实际实现需要使用:
        - Google Speech Recognition
        - Whisper
        - Vosk
        - Coqui STT
        等语音识别引擎
        """
        # 简化实现：返回空字符串
        # 实际需要调用ASR引擎
        return ""

    def _calculate_confidence(self, text: str) -> float:
        """
        计算识别置信度

        Args:
            text: 识别的文本

        Returns:
            置信度 (0.0 - 1.0)
        """
        if not text:
            return 0.0

        # 简化的置信度计算
        # 实际实现需要考虑多种因素
        base_confidence = 0.9

        # 文本长度因素
        if len(text) < 3:
            base_confidence *= 0.8
        elif len(text) > 100:
            base_confidence *= 0.9

        return base_confidence

    # ============== Natural Language Control ==============

    def process_natural_language(self, text: str) -> Dict[str, Any]:
        """
        处理自然语言命令

        Args:
            text: 输入文本

        Returns:
            解析结果包含 action 和 params
        """
        text_lower = text.lower().strip()

        # 检查是否是快捷键
        for shortcut in self.shortcuts.values():
            if text_lower == shortcut.phrase.lower():
                return {
                    "action": shortcut.action,
                    "params": shortcut.params,
                    "type": "shortcut",
                    "matched_phrase": shortcut.phrase
                }

        # 匹配自然语言模式
        for action, patterns in self.nl_commands.items():
            for pattern in patterns:
                if pattern.lower() in text_lower:
                    return {
                        "action": action,
                        "params": self._extract_params(text, action),
                        "type": "nl_command",
                        "matched_pattern": pattern
                    }

        # 尝试提取工作流名称
        workflow_match = self._extract_workflow_name(text)
        if workflow_match:
            return {
                "action": "launch_workflow",
                "params": {"workflow_name": workflow_match},
                "type": "workflow_launch"
            }

        # 无法理解
        return {
            "action": "unknown",
            "params": {"original_text": text},
            "type": "unknown"
        }

    def _extract_params(self, text: str, action: str) -> Dict[str, Any]:
        """
        从文本中提取参数

        Args:
            text: 输入文本
            action: 动作类型

        Returns:
            参数字典
        """
        params = {}

        # 提取数字
        numbers = re.findall(r'\d+', text)
        if numbers:
            params['numbers'] = [int(n) for n in numbers]

        # 提取引号内的内容
        quoted = re.findall(r'["\']([^"\']+)["\']', text)
        if quoted:
            params['quoted_text'] = quoted

        return params

    def _extract_workflow_name(self, text: str) -> Optional[str]:
        """
        从文本中提取工作流名称

        Args:
            text: 输入文本

        Returns:
            工作流名称或None
        """
        # 模式: "start <workflow_name>" 或 "launch <workflow_name>"
        patterns = [
            r'(?:start|launch|run|execute)\s+(?:workflow\s+)?(.+)',
            r'(?:workflow\s+)?named?\s+(.+)',
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                name = match.group(1).strip()
                # 清理可能的标点
                name = re.sub(r'[.,!?;:\s]+$', '', name)
                return name

        return None

    # ============== Voice Dictation ==============

    def dictate_text(
        self,
        audio_data: bytes,
        target_variable: str,
        language: Language = None
    ) -> Optional[str]:
        """
        听写文本到变量

        Args:
            audio_data: 音频数据
            target_variable: 目标变量名
            language: 语言

        Returns:
            听写的文本
        """
        if language is None:
            language = self.language

        command = self.recognize_speech(audio_data, language)

        if command:
            self.logger.info(f"Dictated to {target_variable}: {command.text}")
            return command.text

        return None

    def set_dictation_variable(
        self,
        variable_name: str,
        value: str
    ) -> None:
        """
        设置听写变量

        Args:
            variable_name: 变量名
            value: 值
        """
        # 存储在命令元数据中
        if not hasattr(self, '_dictation_variables'):
            self._dictation_variables = {}
        self._dictation_variables[variable_name] = value
        self.logger.debug(f"Dictation variable set: {variable_name} = {value}")

    # ============== Voice Feedback (TTS) ==============

    def speak(
        self,
        text: str,
        language: Language = None,
        wait: bool = True
    ) -> None:
        """
        语音合成输出

        Args:
            text: 要说的文本
            language: 语言
            wait: 是否等待完成
        """
        if not self.tts_enabled or not self.feedback.enabled:
            return

        if language is None:
            language = self.language

        self.state = VoiceControlState.SPEAKING

        try:
            # 实际实现需要使用TTS引擎如:
            # - gTTS
            # - pyttsx3
            # - Coqui TTS
            # - ElevenLabs API
            self.logger.info(f"TTS: {text}")

            # 模拟播放
            if wait:
                time.sleep(0.1)  # 模拟处理时间

        except Exception as e:
            self.logger.error(f"TTS error: {e}")
        finally:
            self.state = VoiceControlState.IDLE

    def speak_confirmation(self, action: str) -> None:
        """
        说动作确认

        Args:
            action: 动作描述
        """
        confirmations = {
            "start": "Starting workflow",
            "stop": "Stopping workflow",
            "pause": "Pausing workflow",
            "resume": "Resuming workflow",
        }
        message = confirmations.get(action, f"Executing {action}")
        self.speak(message)

    def speak_status(self, status: Dict[str, Any]) -> None:
        """
        播报状态

        Args:
            status: 状态信息
        """
        if "running" in status:
            count = status["running"]
            if count == 0:
                self.speak("No workflows running")
            elif count == 1:
                self.speak("One workflow running")
            else:
                self.speak(f"{count} workflows running")

    # ============== Voice Shortcuts ==============

    def add_shortcut(
        self,
        phrase: str,
        action: str,
        params: Dict[str, Any] = None,
        language: Language = None
    ) -> VoiceShortcut:
        """
        添加语音快捷键

        Args:
            phrase: 短语
            action: 动作
            params: 参数
            language: 语言

        Returns:
            创建的快捷键
        """
        if language is None:
            language = self.language

        shortcut_id = self._generate_shortcut_id()

        shortcut = VoiceShortcut(
            shortcut_id=shortcut_id,
            phrase=phrase,
            action=action,
            params=params or {},
            language=language
        )

        self.shortcuts[shortcut_id] = shortcut
        self.logger.info(f"Added shortcut: {phrase} -> {action}")

        return shortcut

    def remove_shortcut(self, shortcut_id: str) -> bool:
        """
        删除语音快捷键

        Args:
            shortcut_id: 快捷键ID

        Returns:
            是否成功删除
        """
        if shortcut_id in self.shortcuts:
            del self.shortcuts[shortcut_id]
            self.logger.info(f"Removed shortcut: {shortcut_id}")
            return True
        return False

    def get_shortcut(self, phrase: str, language: Language = None) -> Optional[VoiceShortcut]:
        """
        根据短语获取快捷键

        Args:
            phrase: 短语
            language: 语言

        Returns:
            快捷键或None
        """
        if language is None:
            language = self.language

        for shortcut in self.shortcuts.values():
            if shortcut.phrase.lower() == phrase.lower() and shortcut.language == language:
                return shortcut
        return None

    def execute_shortcut(self, shortcut_id: str) -> bool:
        """
        执行快捷键

        Args:
            shortcut_id: 快捷键ID

        Returns:
            是否成功执行
        """
        if shortcut_id not in self.shortcuts:
            return False

        shortcut = self.shortcuts[shortcut_id]
        shortcut.usage_count += 1

        # 注册命令处理器
        if shortcut.action in self.commands:
            handler = self.commands[shortcut.action]
            handler(shortcut.params)
            return True

        return False

    def list_shortcuts(self, language: Language = None) -> List[VoiceShortcut]:
        """
        列出语音快捷键

        Args:
            language: 语言过滤

        Returns:
            快捷键列表
        """
        if language is None:
            return list(self.shortcuts.values())
        return [s for s in self.shortcuts.values() if s.language == language]

    # ============== Workflow Voice Launch ==============

    def register_workflow_voice_launch(
        self,
        workflow_id: str,
        workflow_name: str,
        voice_command: str = None,
        auto_confirm: bool = False
    ) -> WorkflowVoiceLaunch:
        """
        注册工作流的语音启动

        Args:
            workflow_id: 工作流ID
            workflow_name: 工作流名称
            voice_command: 语音命令
            auto_confirm: 自动确认

        Returns:
            创建的语音启动配置
        """
        if voice_command is None:
            voice_command = f"start {workflow_name}"

        launch = WorkflowVoiceLaunch(
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            voice_command=voice_command,
            auto_confirm=auto_confirm
        )

        self.workflow_launches[workflow_id] = launch
        self.logger.info(f"Registered voice launch for workflow: {workflow_name}")

        return launch

    def unregister_workflow_voice_launch(self, workflow_id: str) -> bool:
        """
        注销工作流的语音启动

        Args:
            workflow_id: 工作流ID

        Returns:
            是否成功注销
        """
        if workflow_id in self.workflow_launches:
            del self.workflow_launches[workflow_id]
            return True
        return False

    def launch_workflow_by_voice(self, voice_command: str) -> Optional[str]:
        """
        通过语音启动工作流

        Args:
            voice_command: 语音命令

        Returns:
            工作流ID或None
        """
        voice_command_lower = voice_command.lower().strip()

        for launch in self.workflow_launches.values():
            if launch.voice_command.lower() == voice_command_lower:
                self.logger.info(f"Launching workflow by voice: {launch.workflow_name}")

                if self.enable_voice_feedback:
                    self.speak_confirmation("start")

                if self.on_workflow_launch:
                    self.on_workflow_launch(launch.workflow_id)

                return launch.workflow_id

        # 尝试从自然语言提取工作流名称
        result = self.process_natural_language(voice_command)
        if result["action"] == "launch_workflow":
            workflow_name = result["params"].get("workflow_name")
            if workflow_name:
                # 查找匹配的工作流
                for launch in self.workflow_launches.values():
                    if workflow_name.lower() in launch.workflow_name.lower():
                        if self.on_workflow_launch:
                            self.on_workflow_launch(launch.workflow_id)
                        return launch.workflow_id

        return None

    # ============== Voice Training ==============

    def create_training_profile(
        self,
        user_id: str,
        language: Language = None
    ) -> VoiceTrainingProfile:
        """
        创建语音训练档案

        Args:
            user_id: 用户ID
            language: 语言

        Returns:
            创建的训练档案
        """
        if language is None:
            language = self.language

        profile = VoiceTrainingProfile(
            user_id=user_id,
            language=language,
            last_updated=time.time()
        )

        self.training_profiles[user_id] = profile
        self.current_profile = profile

        self.logger.info(f"Created training profile for user: {user_id}")

        return profile

    def load_training_profile(self, user_id: str) -> Optional[VoiceTrainingProfile]:
        """
        加载语音训练档案

        Args:
            user_id: 用户ID

        Returns:
            训练档案或None
        """
        if user_id in self.training_profiles:
            self.current_profile = self.training_profiles[user_id]
            self.logger.info(f"Loaded training profile for user: {user_id}")
            return self.current_profile
        return None

    def update_training_profile(
        self,
        user_id: str,
        adaptation_data: Dict[str, Any] = None
    ) -> bool:
        """
        更新语音训练档案

        Args:
            user_id: 用户ID
            adaptation_data: 新的适应数据

        Returns:
            是否更新成功
        """
        if user_id not in self.training_profiles:
            return False

        profile = self.training_profiles[user_id]

        if adaptation_data:
            profile.adaptation_data.update(adaptation_data)

        profile.last_updated = time.time()

        self.logger.info(f"Updated training profile for user: {user_id}")

        return True

    def add_adaptation_sample(
        self,
        audio_data: bytes,
        text: str
    ) -> bool:
        """
        添加适应样本

        Args:
            audio_data: 音频数据
            text: 对应文本

        Returns:
            是否添加成功
        """
        if not self.current_profile:
            self.logger.warning("No current training profile")
            return False

        if "samples" not in self.current_profile.adaptation_data:
            self.current_profile.adaptation_data["samples"] = []

        sample = {
            "audio_hash": hash(audio_data),
            "text": text,
            "timestamp": time.time()
        }

        self.current_profile.adaptation_data["samples"].append(sample)
        self.current_profile.last_updated = time.time()

        self.logger.debug(f"Added adaptation sample: {text}")

        return True

    def train_voice_model(self, user_id: str) -> bool:
        """
        训练语音模型

        Args:
            user_id: 用户ID

        Returns:
            是否训练成功
        """
        if user_id not in self.training_profiles:
            return False

        profile = self.training_profiles[user_id]

        samples = profile.adaptation_data.get("samples", [])

        if len(samples) < 10:
            self.logger.warning(f"Not enough samples for training: {len(samples)}")
            return False

        # 实际实现需要:
        # 1. 使用语音数据训练声学模型
        # 2. 更新语言模型
        # 3. 保存模型文件
        # 这里只是模拟

        self.logger.info(f"Training voice model for user: {user_id}")
        self.logger.info(f"Using {len(samples)} samples")

        # 模拟训练时间
        time.sleep(0.1)

        profile.adaptation_data["trained"] = True
        profile.last_updated = time.time()

        return True

    # ============== Multi-language Support ==============

    def set_language(self, language: Language) -> None:
        """
        设置语言

        Args:
            language: 语言
        """
        self.language = language
        self.logger.info(f"Language set to: {language.value}")

    def get_available_languages(self) -> List[Language]:
        """
        获取支持的语言列表

        Returns:
            语言列表
        """
        return list(Language)

    def add_language_command(
        self,
        language: Language,
        command_key: str,
        phrases: List[str]
    ) -> None:
        """
        添加语言命令

        Args:
            language: 语言
            command_key: 命令键
            phrases: 短语列表
        """
        if language not in self.language_models:
            self.language_models[language] = {
                "name": language.value,
                "commands": {},
                "wake_words": [],
                "phonetic_model": None
            }

        self.language_models[language]["commands"][command_key] = phrases

        self.logger.debug(f"Added commands for {language.value}: {command_key}")

    def recognize_in_language(
        self,
        audio_data: bytes,
        language: Language
    ) -> Optional[VoiceCommand]:
        """
        使用指定语言识别

        Args:
            audio_data: 音频数据
            language: 语言

        Returns:
            语音命令
        """
        return self.recognize_speech(audio_data, language)

    # ============== Command Registration ==============

    def register_command(
        self,
        command_name: str,
        handler: Callable,
        description: str = None
    ) -> None:
        """
        注册命令处理器

        Args:
            command_name: 命令名称
            handler: 处理函数
            description: 描述
        """
        self.commands[command_name] = handler
        self.logger.info(f"Registered command: {command_name}")

    def unregister_command(self, command_name: str) -> bool:
        """
        注销命令处理器

        Args:
            command_name: 命令名称

        Returns:
            是否成功注销
        """
        if command_name in self.commands:
            del self.commands[command_name]
            return True
        return False

    def execute_command(
        self,
        command_name: str,
        params: Dict[str, Any] = None
    ) -> Any:
        """
        执行命令

        Args:
            command_name: 命令名称
            params: 参数

        Returns:
            命令执行结果
        """
        if command_name not in self.commands:
            self.logger.warning(f"Command not found: {command_name}")
            return None

        handler = self.commands[command_name]

        try:
            if params:
                return handler(params)
            return handler()
        except Exception as e:
            self.logger.error(f"Command execution error: {e}")
            if self.on_error:
                self.on_error(str(e))
            return None

    # ============== Main Voice Loop ==============

    def start_listening(self, audio_stream: Any = None) -> None:
        """
        开始监听

        Args:
            audio_stream: 音频流
        """
        self.is_listening = True
        self.logger.info("Started voice control listening")

        # 在后台线程运行
        thread = threading.Thread(
            target=self._voice_loop,
            args=(audio_stream,),
            daemon=True
        )
        thread.start()

    def stop_listening(self) -> None:
        """停止监听"""
        self.is_listening = False
        self.logger.info("Stopped voice control listening")

    def _voice_loop(self, audio_stream: Any = None) -> None:
        """语音循环"""
        while self.is_listening:
            try:
                # 等待唤醒词
                wake_detected = self.listen_for_wake_word(audio_stream)

                if wake_detected and self.is_listening:
                    # 听命令
                    self._listen_for_command(audio_stream)

            except Exception as e:
                self.logger.error(f"Voice loop error: {e}")
                time.sleep(1)

    def _listen_for_command(self, audio_stream: Any = None) -> None:
        """监听命令"""
        silence_start = None

        while self.is_active and self.is_listening:
            try:
                # 读取音频
                if audio_stream:
                    audio_data = self._read_audio_chunk(audio_stream)
                else:
                    audio_data = b'\x00' * self.chunk_size * 2  # 模拟静音

                if not audio_data:
                    continue

                # 检测语音活动
                activity = self.detect_voice_activity(audio_data)

                if activity == VoiceActivity.SPEECH:
                    silence_start = None
                    # 识别命令
                    command = self.recognize_speech(audio_data)
                    if command:
                        self._handle_command(command)
                        self.is_active = False
                        break

                elif activity == VoiceActivity.SILENCE:
                    if silence_start is None:
                        silence_start = time.time()
                    elif time.time() - silence_start > self.silence_timeout:
                        self.logger.debug("Silence timeout")
                        self.is_active = False
                        break

            except Exception as e:
                self.logger.error(f"Command listening error: {e}")
                break

    def _read_audio_chunk(self, audio_stream: Any) -> bytes:
        """
        读取音频块

        Args:
            audio_stream: 音频流

        Returns:
            音频数据
        """
        try:
            return audio_stream.read(self.chunk_size)
        except Exception:
            return b''

    def _handle_command(self, command: VoiceCommand) -> None:
        """
        处理命令

        Args:
            command: 语音命令
        """
        self.logger.info(f"Handling command: {command.text}")

        # 处理自然语言
        result = self.process_natural_language(command.text)

        if result["type"] == "shortcut":
            self.execute_shortcut(result["action"])
        elif result["action"] in self.commands:
            self.execute_command(result["action"], result["params"])
        elif result["action"] == "launch_workflow":
            self.launch_workflow_by_voice(command.text)
        elif result["action"] == "unknown":
            self.speak("I didn't understand that, please repeat")

    # ============== Utility Methods ==============

    def _generate_command_id(self) -> str:
        """生成命令ID"""
        return f"cmd_{int(time.time()*1000)}"

    def _generate_shortcut_id(self) -> str:
        """生成快捷键ID"""
        return f"shortcut_{len(self.shortcuts) + 1}"

    def _add_to_history(self, command: VoiceCommand) -> None:
        """
        添加到历史

        Args:
            command: 语音命令
        """
        self.command_history.append(command)

        if len(self.command_history) > self.max_history_size:
            self.command_history.pop(0)

    def get_command_history(
        self,
        limit: int = 100,
        language: Language = None
    ) -> List[VoiceCommand]:
        """
        获取命令历史

        Args:
            limit: 限制数量
            language: 语言过滤

        Returns:
            命令历史
        """
        history = self.command_history[-limit:]

        if language:
            history = [c for c in history if c.language == language]

        return history

    def clear_history(self) -> None:
        """清除历史"""
        self.command_history.clear()
        self.logger.info("Command history cleared")

    def get_status(self) -> Dict[str, Any]:
        """
        获取状态

        Returns:
            状态字典
        """
        return {
            "state": self.state.value,
            "is_active": self.is_active,
            "is_listening": self.is_listening,
            "language": self.language.value,
            "wake_word": self.wake_word,
            "shortcuts_count": len(self.shortcuts),
            "workflows_registered": len(self.workflow_launches),
            "history_size": len(self.command_history),
            "vad_enabled": self.enable_vad,
            "tts_enabled": self.tts_enabled,
        }

    # ============== Audio File Operations ==============

    def load_audio_file(self, file_path: str) -> Optional[bytes]:
        """
        加载音频文件

        Args:
            file_path: 文件路径

        Returns:
            音频数据
        """
        try:
            with wave.open(file_path, 'rb') as wf:
                frames = wf.readframes(wf.getnframes())
                return frames
        except Exception as e:
            self.logger.error(f"Audio file load error: {e}")
            return None

    def save_audio_file(self, file_path: str, audio_data: bytes) -> bool:
        """
        保存音频文件

        Args:
            file_path: 文件路径
            audio_data: 音频数据

        Returns:
            是否保存成功
        """
        try:
            with wave.open(file_path, 'wb') as wf:
                wf.setnchannels(self.channels)
                wf.setsampwidth(2)  # 16-bit
                wf.setframerate(self.sample_rate)
                wf.writeframes(audio_data)
            return True
        except Exception as e:
            self.logger.error(f"Audio file save error: {e}")
            return False

    # ============== Configuration ==============

    def save_config(self, file_path: str) -> bool:
        """
        保存配置

        Args:
            file_path: 文件路径

        Returns:
            是否保存成功
        """
        try:
            config = {
                "wake_word": self.wake_word,
                "language": self.language.value,
                "enable_vad": self.enable_vad,
                "enable_voice_feedback": self.enable_voice_feedback,
                "vad_threshold": self.vad_threshold,
                "silence_timeout": self.silence_timeout,
                "speech_timeout": self.speech_timeout,
                "feedback": {
                    "enabled": self.feedback.enabled,
                    "volume": self.feedback.volume,
                    "rate": self.feedback.rate,
                    "pitch": self.feedback.pitch,
                },
                "shortcuts": [
                    {
                        "shortcut_id": s.shortcut_id,
                        "phrase": s.phrase,
                        "action": s.action,
                        "params": s.params,
                        "language": s.language.value,
                    }
                    for s in self.shortcuts.values()
                ],
            }

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)

            return True

        except Exception as e:
            self.logger.error(f"Config save error: {e}")
            return False

    def load_config(self, file_path: str) -> bool:
        """
        加载配置

        Args:
            file_path: 文件路径

        Returns:
            是否加载成功
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                config = json.load(f)

            self.wake_word = config.get("wake_word", self.wake_word)
            self.language = Language(config.get("language", self.language.value))
            self.enable_vad = config.get("enable_vad", self.enable_vad)
            self.enable_voice_feedback = config.get("enable_voice_feedback", self.enable_voice_feedback)
            self.vad_threshold = config.get("vad_threshold", self.vad_threshold)
            self.silence_timeout = config.get("silence_timeout", self.silence_timeout)
            self.speech_timeout = config.get("speech_timeout", self.speech_timeout)

            feedback_config = config.get("feedback", {})
            self.feedback = VoiceFeedback(
                enabled=feedback_config.get("enabled", self.feedback.enabled),
                volume=feedback_config.get("volume", self.feedback.volume),
                rate=feedback_config.get("rate", self.feedback.rate),
                pitch=feedback_config.get("pitch", self.feedback.pitch),
            )

            # 加载快捷键
            shortcuts_config = config.get("shortcuts", [])
            self.shortcuts.clear()
            for sc in shortcuts_config:
                shortcut = VoiceShortcut(
                    shortcut_id=sc["shortcut_id"],
                    phrase=sc["phrase"],
                    action=sc["action"],
                    params=sc.get("params", {}),
                    language=Language(sc.get("language", "en")),
                )
                self.shortcuts[shortcut.shortcut_id] = shortcut

            return True

        except Exception as e:
            self.logger.error(f"Config load error: {e}")
            return False

    def reset(self) -> None:
        """重置为默认配置"""
        self.wake_word = "hey assistant"
        self.language = Language.ENGLISH
        self.enable_vad = True
        self.enable_voice_feedback = True
        self.vad_threshold = 0.5
        self.silence_timeout = 2.0
        self.speech_timeout = 30.0
        self.feedback = VoiceFeedback()
        self.is_active = False
        self.is_listening = False
        self.state = VoiceControlState.IDLE
        self._initialize_default_shortcuts()
        self.logger.info("Voice control reset to defaults")
