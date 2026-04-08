"""API Schema Action Module. Manages API schemas and generates mock data."""
import sys, os, random, string, uuid as uuid_mod
from typing import Any, Optional
from dataclasses import dataclass, field
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class SchemaField:
    name: str; field_type: str; required: bool = False; default: Any = None
    min_value: Optional[float] = None; max_value: Optional[float] = None
    min_length: Optional[int] = None; max_length: Optional[int] = None
    pattern: Optional[str] = None; enum_values: list = field(default_factory=list)

class APISchemaAction(BaseAction):
    action_type = "api_schema"; display_name = "API Schema管理"
    description = "管理API Schema"
    def __init__(self) -> None: super().__init__(); self._schemas = {}
    def register_schema(self, name: str, fields: list) -> None:
        self._schemas[name] = {"fields": fields}
    def _generate_mock(self, field_def: SchemaField) -> Any:
        ft = field_def.field_type
        if ft == "string":
            length = random.randint(field_def.min_length or 3, field_def.max_length or 20)
            return ''.join(random.choices(string.ascii_letters, k=length))
        elif ft == "int":
            return random.randint(int(field_def.min_value or 0), int(field_def.max_value or 1000))
        elif ft == "float":
            return round(random.uniform(field_def.min_value or 0.0, field_def.max_value or 100.0), 2)
        elif ft == "bool": return random.choice([True, False])
        elif ft == "email": return f"user{random.randint(1,999)}@example.com"
        elif ft == "uuid": return str(uuid_mod.uuid4())
        elif ft == "enum": return random.choice(field_def.enum_values) if field_def.enum_values else None
        elif ft == "array": return [self._generate_mock(field_def) for _ in range(random.randint(1,5))]
        return None
    def execute(self, context: Any, params: dict) -> ActionResult:
        mode = params.get("mode", "register")
        schema_name = params.get("schema_name", "default")
        if mode == "register":
            fields_data = params.get("fields", [])
            fields = [SchemaField(name=fd.get("name",""), field_type=fd.get("field_type","string"),
                                  required=fd.get("required",False), default=fd.get("default"),
                                  min_value=fd.get("min_value"), max_value=fd.get("max_value"),
                                  min_length=fd.get("min_length"), max_length=fd.get("max_length"),
                                  pattern=fd.get("pattern"), enum_values=fd.get("enum_values",[]))
                     for fd in fields_data]
            self.register_schema(schema_name, fields)
            return ActionResult(success=True, message=f"Schema '{schema_name}' registered")
        if mode == "mock":
            if schema_name not in self._schemas: return ActionResult(success=False, message=f"Schema '{schema_name}' not found")
            count = params.get("count", 1)
            schema = self._schemas[schema_name]
            records = []
            for _ in range(count):
                record = {}
                for fd in schema["fields"]:
                    if fd.required or random.random() > 0.3: record[fd.name] = self._generate_mock(fd)
                records.append(record)
            return ActionResult(success=True, message=f"Generated {count} mock records", data={"records": records})
        if schema_name not in self._schemas: return ActionResult(success=False, message=f"Schema '{schema_name}' not found")
        payload = params.get("payload", {}); schema = self._schemas[schema_name]; errors = []
        for fd in schema["fields"]:
            value = payload.get(fd.name)
            if fd.required and (value is None or value == ""): errors.append(f"Missing required: {fd.name}")
        return ActionResult(success=len(errors)==0, message=f"Validation: {'PASSED' if not errors else f'FAILED ({len(errors)})'}",
                          data={"valid": len(errors)==0, "errors": errors})
