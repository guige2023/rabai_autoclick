# Data Directory

This directory stores application data and workflow scenes.

## Files

| File | Description |
|------|-------------|
| `workflow_scenes.json` | Scene-based workflow definitions |

## Directory Structure

```
data/
├── README.md              # This file
└── workflow_scenes.json  # Scene definitions

# Created at runtime:
├── action_history.json    # User action history
├── diagnostics/          # Diagnostics reports
├── shared_workflows/     # Shared workflow links
└── recordings/           # Screen recordings
```

## Scene Format

```json
{
  "scenes": [
    {
      "scene_id": "morning_routine",
      "name": "Morning Routine",
      "description": "Automated morning tasks",
      "icon": "🌅",
      "status": "active",
      "workflows": [
        {
          "workflow_id": "wf_001",
          "workflow_name": "Turn on lights",
          "enabled": true,
          "delay": 0.0,
          "order": 1
        }
      ],
      "schedule": {
        "enabled": true,
        "time": "07:00",
        "days": ["monday", "tuesday", "wednesday", "thursday", "friday"],
        "timezone": "Asia/Shanghai"
      },
      "tags": ["morning", "automation"],
      "usage_count": 42
    }
  ]
}
```

## Backup

This directory should be included in backups as it contains:
- User-created workflows
- Recorded action history
- Custom scenes and schedules

## Cleanup

Old data can be safely deleted if:
- You no longer need the action history
- Scenes are not being used
- Diagnostics reports are old
