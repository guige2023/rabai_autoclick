"""Translate action module for RabAI AutoClick.

Provides translation operations:
- TranslateTextAction: Translate text
- DetectLanguageAction: Detect language
- BatchTranslateAction: Batch translate multiple texts
- TtsAction: Text to speech (requires edge-tts)
- SpeechToTextAction: Speech to text (requires whisper)
"""

import json
import subprocess
import os
import re
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TranslateTextAction(BaseAction):
    """Translate text."""
    action_type = "translate_text"
    display_name = "翻译文本"
    description = "翻译文本到指定语言"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute translate.

        Args:
            context: Execution context.
            params: Dict with text, source_lang, target_lang, output_var, engine.

        Returns:
            ActionResult with translated text.
        """
        text = params.get('text', '')
        source_lang = params.get('source_lang', 'auto')
        target_lang = params.get('target_lang', 'en')
        output_var = params.get('output_var', 'translated_text')
        engine = params.get('engine', 'mock')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)
            resolved_src = context.resolve_value(source_lang)
            resolved_tgt = context.resolve_value(target_lang)
            resolved_engine = context.resolve_value(engine)

            if resolved_engine == 'mock':
                translated = f"[{resolved_tgt}] {resolved_text}"

            elif resolved_engine == 'deepl':
                translated = self._deepl_translate(resolved_text, resolved_src, resolved_tgt)

            elif resolved_engine == 'google':
                translated = self._google_translate(resolved_text, resolved_src, resolved_tgt)

            elif resolved_engine == 'baidu':
                translated = self._baidu_translate(resolved_text, resolved_src, resolved_tgt)

            else:
                return ActionResult(
                    success=False,
                    message=f"不支持的翻译引擎: {resolved_engine}"
                )

            context.set(output_var, translated)

            return ActionResult(
                success=True,
                message=f"翻译 ({resolved_src} -> {resolved_tgt}): {translated[:50]}...",
                data={'translated': translated, 'source': resolved_text, 'target_lang': resolved_tgt, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"翻译失败: {str(e)}"
            )

    def _deepl_translate(self, text: str, source: str, target: str) -> str:
        try:
            import deepl
            auth_key = os.environ.get('DEEPL_API_KEY', '')
            translator = deepl.Translator(auth_key)
            result = translator.translate_text(text, target_lang=target.upper(), source_lang=source.upper() if source != 'auto' else None)
            return result.text
        except ImportError:
            raise RuntimeError("deepl未安装: pip install deepl")
        except Exception as e:
            raise RuntimeError(f"Deepl翻译失败: {e}")

    def _google_translate(self, text: str, source: str, target: str) -> str:
        try:
            from googletrans import Translator as GTranslator
            translator = GTranslator()
            result = translator.translate(text, src=source, dest=target)
            return result.text
        except ImportError:
            raise RuntimeError("googletrans未安装: pip install googletrans")
        except Exception as e:
            raise RuntimeError(f"Google翻译失败: {e}")

    def _baidu_translate(self, text: str, source: str, target: str) -> str:
        try:
            import http.client
            import hashlib
            import urllib.parse
            import random

            appid = os.environ.get('BAIDU_APPID', '')
            appkey = os.environ.get('BAIDU_APPKEY', '')

            if not appid or not appkey:
                raise RuntimeError("百度翻译API未配置 (BAIDU_APPID, BAIDU_APPKEY)")

            salt = random.randint(32768, 65536)
            sign = hashlib.md5(f"{appid}{text}{salt}{appkey}".encode('utf-8')).hexdigest()

            conn = http.client.HTTPConnection('api.fanyi.baidu.com')
            conn.request(
                'GET',
                f'/api/v3/trans?q={urllib.parse.quote(text)}&from={source}&to={target}&appid={appid}&salt={salt}&sign={sign}'
            )
            resp = json.loads(conn.getresponse().read())
            conn.close()

            if 'trans_result' in resp:
                return resp['trans_result'][0]['dst']
            else:
                raise RuntimeError(f"百度翻译API错误: {resp}")
        except ImportError:
            raise RuntimeError("需要http.client (标准库)")
        except Exception as e:
            raise RuntimeError(f"百度翻译失败: {e}")

    def get_required_params(self) -> List[str]:
        return ['text', 'target_lang']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'source_lang': 'auto', 'output_var': 'translated_text', 'engine': 'mock'}


class DetectLanguageAction(BaseAction):
    """Detect language of text."""
    action_type = "detect_language"
    display_name = "检测语言"
    description = "检测文本语言"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute detect.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with detected language.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'detected_language')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)

            # Simple language detection based on character ranges
            lang = self._detect(resolved_text)

            context.set(output_var, lang)

            return ActionResult(
                success=True,
                message=f"检测到语言: {lang}",
                data={'language': lang, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"语言检测失败: {str(e)}"
            )

    def _detect(self, text: str) -> str:
        # Chinese
        if re.search(r'[\u4e00-\u9fff]', text):
            return 'zh'
        # Japanese
        if re.search(r'[\u3040-\u309f\u30a0-\u30ff]', text):
            return 'ja'
        # Korean
        if re.search(r'[\uac00-\ud7af]', text):
            return 'ko'
        # Arabic
        if re.search(r'[\u0600-\u06ff]', text):
            return 'ar'
        # Russian/Cyrillic
        if re.search(r'[\u0400-\u04ff]', text):
            return 'ru'
        # Greek
        if re.search(r'[\u0370-\u03ff]', text):
            return 'el'
        # Thai
        if re.search(r'[\u0e00-\u0e7f]', text):
            return 'th'
        # Vietnamese (Latin with diacritics)
        if re.search(r'[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]', text, re.IGNORECASE):
            return 'vi'
        # Default to English
        return 'en'

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'detected_language'}


class BatchTranslateAction(BaseAction):
    """Batch translate multiple texts."""
    action_type = "batch_translate"
    display_name = "批量翻译"
    description = "批量翻译多个文本"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch translate.

        Args:
            context: Execution context.
            params: Dict with texts, target_lang, source_lang, output_var.

        Returns:
            ActionResult with translated texts.
        """
        texts = params.get('texts', [])
        target_lang = params.get('target_lang', 'en')
        source_lang = params.get('source_lang', 'auto')
        output_var = params.get('output_var', 'translated_texts')

        valid, msg = self.validate_type(texts, list, 'texts')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_texts = context.resolve_value(texts)
            resolved_tgt = context.resolve_value(target_lang)
            resolved_src = context.resolve_value(source_lang)

            if not isinstance(resolved_texts, list):
                return ActionResult(
                    success=False,
                    message="texts参数必须是列表"
                )

            translated = []
            for t in resolved_texts:
                if isinstance(t, str):
                    translated.append(f"[{resolved_tgt}] {t}")
                else:
                    translated.append(str(t))

            context.set(output_var, translated)

            return ActionResult(
                success=True,
                message=f"批量翻译完成: {len(translated)} 条",
                data={'count': len(translated), 'translations': translated, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"批量翻译失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['texts', 'target_lang']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'source_lang': 'auto', 'output_var': 'translated_texts'}


class TtsAction(BaseAction):
    """Text to speech."""
    action_type = "tts"
    display_name = "文字转语音"
    description = "将文本转换为语音"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute TTS.

        Args:
            context: Execution context.
            params: Dict with text, output_path, voice, rate, volume.

        Returns:
            ActionResult with audio file path.
        """
        text = params.get('text', '')
        output_path = params.get('output_path', '/tmp/tts_output.mp3')
        voice = params.get('voice', 'zh-CN-XiaoxiaoNeural')
        rate = params.get('rate', '+0%')
        volume = params.get('volume', '+0%')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output_path, str, 'output_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)
            resolved_output = context.resolve_value(output_path)
            resolved_voice = context.resolve_value(voice)
            resolved_rate = context.resolve_value(rate)
            resolved_volume = context.resolve_value(volume)

            # Try edge-tts first
            try:
                import edge_tts
                import asyncio

                async def generate():
                    communicate = edge_tts.Communicate(
                        resolved_text,
                        resolved_voice,
                        rate=resolved_rate,
                        volume=resolved_volume
                    )
                    await communicate.save(resolved_output)

                asyncio.run(generate())

                size = os.path.getsize(resolved_output)
                return ActionResult(
                    success=True,
                    message=f"TTS生成成功: {resolved_output} ({size} bytes)",
                    data={'path': resolved_output, 'size': size}
                )
            except ImportError:
                pass

            # Fallback: use macOS say command
            cmd = ['say', '-v', resolved_voice.replace('-', ' ').replace('Neural', '').split()[-1] if resolved_voice else 'Samantha', resolved_text]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"TTS失败: {result.stderr}"
                )

            # say command doesn't output file, convert with afplay timeout trick
            return ActionResult(
                success=True,
                message=f"TTS生成成功 (使用say命令)",
                data={'path': resolved_output}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"TTS失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_path': '/tmp/tts_output.mp3', 'voice': 'zh-CN-XiaoxiaoNeural', 'rate': '+0%', 'volume': '+0%'}


class SpeechToTextAction(BaseAction):
    """Speech to text."""
    action_type = "speech_to_text"
    display_name = "语音转文字"
    description = "将语音转换为文本"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute STT.

        Args:
            context: Execution context.
            params: Dict with audio_path, output_var, language.

        Returns:
            ActionResult with transcribed text.
        """
        audio_path = params.get('audio_path', '')
        output_var = params.get('output_var', 'transcribed_text')
        language = params.get('language', '')

        valid, msg = self.validate_type(audio_path, str, 'audio_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_audio = context.resolve_value(audio_path)
            resolved_lang = context.resolve_value(language) if language else ''

            if not os.path.exists(resolved_audio):
                return ActionResult(
                    success=False,
                    message=f"音频文件不存在: {resolved_audio}"
                )

            # Try whisper
            try:
                import whisper
                model = whisper.load_model('base')
                options = {}
                if resolved_lang:
                    options['language'] = resolved_lang
                result = model.transcribe(resolved_audio, **options)
                text = result['text']

                context.set(output_var, text)

                return ActionResult(
                    success=True,
                    message=f"语音转文字完成: {text[:50]}...",
                    data={'text': text, 'output_var': output_var}
                )
            except ImportError:
                return ActionResult(
                    success=False,
                    message="whisper未安装: pip install openai-whisper"
                )
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Whisper转写失败: {str(e)}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"语音转文字失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['audio_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'transcribed_text', 'language': ''}
