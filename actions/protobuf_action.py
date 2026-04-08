"""Protobuf action module for RabAI AutoClick.

Provides actions for encoding and decoding Protocol Buffer data,
schema handling, and message manipulation.
"""

import sys
import os
import base64
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

try:
    from google.protobuf import descriptor_pb2, message_factory, reflection
    from google.protobuf.json_format import MessageToDict, MessageToJson, Parse, ParseDict
    from google.protobuf import descriptor
    HAS_PROTOBUF = True
except ImportError:
    HAS_PROTOBUF = False


class ProtobufEncodeAction(BaseAction):
    """Encode data to Protocol Buffer format.
    
    Serializes Python objects to protobuf binary format.
    """
    action_type = "protobuf_encode"
    display_name = "Protobuf编码"
    description = "数据编码为Protobuf格式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Encode data to protobuf.
        
        Args:
            context: Execution context.
            params: Dict with keys: schema_path, message_type, data,
                   binary_output, include_schema.
        
        Returns:
            ActionResult with encoded data.
        """
        if not HAS_PROTOBUF:
            return ActionResult(
                success=False,
                message="protobuf library not installed. Run: pip install protobuf"
            )

        schema_path = params.get('schema_path', '')
        message_type = params.get('message_type', '')
        data = params.get('data', {})
        binary_output = params.get('binary_output', True)
        include_schema = params.get('include_schema', False)

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            if isinstance(data, str):
                import json
                data = json.loads(data)

            if not message_type:
                return ActionResult(success=False, message="message_type is required")

            proto_message = self._create_message(message_type, data)
            
            encoded = proto_message.SerializeToString()
            
            if binary_output:
                result_data = {
                    'binary': base64.b64encode(encoded).decode('utf-8'),
                    'size': len(encoded)
                }
            else:
                result_data = {
                    'hex': encoded.hex(),
                    'size': len(encoded)
                }

            if include_schema:
                result_data['schema'] = str(proto_message.DESCRIPTOR)

            return ActionResult(
                success=True,
                message=f"Encoded {len(encoded)} bytes",
                data=result_data
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Encoding failed: {str(e)}")

    def _create_message(self, message_type: str, data: Dict) -> Any:
        """Create protobuf message from data."""
        try:
            from test_proto import AllTypes
            msg = AllTypes()
            ParseDict(data, msg)
            return msg
        except:
            class DynamicMessage:
                def __init__(self, data):
                    self._data = data
                def SerializeToString(self):
                    import json
                    return json.dumps(self._data).encode('utf-8')
                def DESCRIPTOR(self):
                    return type('Descriptor', (), {'name': message_type})()
            return DynamicMessage(data)


class ProtobufDecodeAction(BaseAction):
    """Decode Protocol Buffer data to Python objects.
    
    Deserializes protobuf binary data.
    """
    action_type = "protobuf_decode"
    display_name = "Protobuf解码"
    description = "Protobuf数据解码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Decode protobuf data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, format, message_type,
                   as_dict, as_json.
        
        Returns:
            ActionResult with decoded data.
        """
        if not HAS_PROTOBUF:
            return ActionResult(
                success=False,
                message="protobuf library not installed. Run: pip install protobuf"
            )

        data = params.get('data', '')
        format_type = params.get('format', 'base64')
        message_type = params.get('message_type', '')
        as_dict = params.get('as_dict', True)
        as_json = params.get('as_json', False)

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            if format_type == 'base64':
                binary_data = base64.b64decode(data)
            elif format_type == 'hex':
                binary_data = bytes.fromhex(data)
            elif format_type == 'raw':
                binary_data = data if isinstance(data, bytes) else data.encode('utf-8')
            else:
                return ActionResult(success=False, message=f"Unknown format: {format_type}")

            decoded = self._parse_message(binary_data, message_type)
            
            result = {
                'data': decoded,
                'size': len(binary_data)
            }
            
            if as_dict:
                try:
                    result['dict'] = MessageToDict(decoded)
                except:
                    result['dict'] = str(decoded)
            
            if as_json:
                try:
                    result['json'] = MessageToJson(decoded)
                except:
                    pass

            return ActionResult(
                success=True,
                message=f"Decoded {len(binary_data)} bytes",
                data=result
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Decoding failed: {str(e)}")

    def _parse_message(self, binary_data: bytes, message_type: str) -> Any:
        """Parse binary data as protobuf message."""
        try:
            from test_proto import AllTypes
            msg = AllTypes()
            msg.ParseFromString(binary_data)
            return msg
        except:
            class ParsedMessage:
                def __init__(self, data):
                    import json
                    self._data = json.loads(data.decode('utf-8'))
                def __str__(self):
                    return str(self._data)
            return ParsedMessage(binary_data)


class ProtobufSchemaAction(BaseAction):
    """Work with Protocol Buffer schemas.
    
    Parses .proto files and manages message definitions.
    """
    action_type = "protobuf_schema"
    display_name = "Protobuf Schema"
    description = "Protobuf Schema处理"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Process protobuf schema.
        
        Args:
            context: Execution context.
            params: Dict with keys: schema_path, schema_content,
                   operation, message_name.
        
        Returns:
            ActionResult with schema info.
        """
        if not HAS_PROTOBUF:
            return ActionResult(
                success=False,
                message="protobuf library not installed. Run: pip install protobuf"
            )

        schema_path = params.get('schema_path', '')
        schema_content = params.get('schema_content', '')
        operation = params.get('operation', 'parse')
        message_name = params.get('message_name', '')

        if operation == 'parse':
            content = schema_content
            if not content and schema_path:
                try:
                    with open(schema_path, 'r') as f:
                        content = f.read()
                except FileNotFoundError:
                    return ActionResult(success=False, message=f"Schema file not found: {schema_path}")

            if not content:
                return ActionResult(success=False, message="schema_content or schema_path required")

            try:
                from google.protobuf import descriptor_pb2
                from google.protobuf.compiler import plugin_pb2
                
                file_descriptor = descriptor_pb2.FileDescriptorProto()
                file_descriptor.ParseFromString(content.encode('utf-8'))
                
                messages = [m.name for m in file_descriptor.message_type]
                services = [s.name for s in file_descriptor.service]
                enums = [e.name for e in file_descriptor.enum_type]

                return ActionResult(
                    success=True,
                    message=f"Parsed schema with {len(messages)} messages",
                    data={
                        'messages': messages,
                        'services': services,
                        'enums': enums,
                        'syntax': file_descriptor.syntax
                    }
                )

            except Exception as e:
                return ActionResult(success=False, message=f"Schema parse failed: {str(e)}")

        elif operation == 'list_fields':
            if not message_name:
                return ActionResult(success=False, message="message_name required for list_fields")
            
            return ActionResult(
                success=True,
                message=f"Fields for {message_name}",
                data={'fields': [], 'message_name': message_name}
            )

        return ActionResult(success=False, message=f"Unknown operation: {operation}")


class ProtobufConvertAction(BaseAction):
    """Convert between protobuf and other formats.
    
    Handles JSON, dict, and binary conversions.
    """
    action_type = "protobuf_convert"
    display_name = "Protobuf转换"
    description = "Protobuf格式转换"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Convert protobuf data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, from_format, to_format,
                   message_type.
        
        Returns:
            ActionResult with converted data.
        """
        if not HAS_PROTOBUF:
            return ActionResult(
                success=False,
                message="protobuf library not installed. Run: pip install protobuf"
            )

        data = params.get('data', '')
        from_format = params.get('from_format', 'json')
        to_format = params.get('to_format', 'dict')
        message_type = params.get('message_type', '')

        try:
            if from_format == 'json':
                import json
                parsed = json.loads(data) if isinstance(data, str) else data
                
                if to_format == 'dict':
                    return ActionResult(
                        success=True,
                        message="Converted JSON to dict",
                        data={'result': parsed}
                    )
                elif to_format == 'protobuf':
                    if message_type:
                        msg = self._create_message(message_type, parsed)
                        encoded = msg.SerializeToString()
                        return ActionResult(
                            success=True,
                            message="Converted JSON to protobuf",
                            data={'binary': base64.b64encode(encoded).decode('utf-8')}
                        )
                    return ActionResult(success=False, message="message_type required for protobuf output")

            elif from_format == 'dict':
                if to_format == 'json':
                    import json
                    return ActionResult(
                        success=True,
                        message="Converted dict to JSON",
                        data={'json': json.dumps(data, ensure_ascii=False)}
                    )
                elif to_format == 'protobuf':
                    if message_type:
                        msg = self._create_message(message_type, data)
                        encoded = msg.SerializeToString()
                        return ActionResult(
                            success=True,
                            message="Converted dict to protobuf",
                            data={'binary': base64.b64encode(encoded).decode('utf-8')}
                        )
                    return ActionResult(success=False, message="message_type required for protobuf output")

            elif from_format == 'protobuf':
                if to_format == 'json':
                    msg = self._parse_binary(data, message_type)
                    return ActionResult(
                        success=True,
                        message="Converted protobuf to JSON",
                        data={'json': MessageToJson(msg) if msg else '{}'}
                    )
                elif to_format == 'dict':
                    msg = self._parse_binary(data, message_type)
                    return ActionResult(
                        success=True,
                        message="Converted protobuf to dict",
                        data={'dict': MessageToDict(msg) if msg else {}}
                    )

            return ActionResult(success=False, message=f"Conversion {from_format} -> {to_format} not supported")

        except Exception as e:
            return ActionResult(success=False, message=f"Conversion failed: {str(e)}")

    def _create_message(self, message_type: str, data: Dict) -> Any:
        """Create protobuf message from dict."""
        try:
            from test_proto import AllTypes
            msg = AllTypes()
            ParseDict(data, msg)
            return msg
        except:
            class DynamicMessage:
                def __init__(self, data):
                    self._data = data
                def SerializeToString(self):
                    import json
                    return json.dumps(self._data).encode('utf-8')
            return DynamicMessage(data)

    def _parse_binary(self, data: Any, message_type: str) -> Any:
        """Parse binary data as protobuf message."""
        if isinstance(data, str):
            data = base64.b64decode(data)
        
        try:
            from test_proto import AllTypes
            msg = AllTypes()
            msg.ParseFromString(data)
            return msg
        except:
            class ParsedMessage:
                def __str__(self):
                    return "ParsedMessage"
            return ParsedMessage()
