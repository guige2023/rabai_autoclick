"""
Workflow Template Library v1.0.0
A curated library of production-ready workflow templates organized in 8 categories.
"""

import json
import uuid
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum


class TemplateCategory(Enum):
    """Template category enumeration"""
    WEB_AUTOMATION = "web_automation"
    FILE_MANAGEMENT = "file_management"
    SOCIAL_MEDIA = "social_media"
    OFFICE_PRODUCTIVITY = "office_productivity"
    SYSTEM_MAINTENANCE = "system_maintenance"
    TESTING = "testing"
    DEVOPS = "devops"
    PERSONAL_AUTOMATION = "personal_automation"


class DifficultyLevel(Enum):
    """Workflow difficulty levels"""
    BEGINNER = "beginner"
    INTERMEDIATE = "intermediate"
    ADVANCED = "advanced"
    EXPERT = "expert"


@dataclass
class WorkflowTemplate:
    """Workflow template structure"""
    id: str
    name: str
    description: str
    version: str
    author: str
    category: str
    tags: List[str]
    estimated_duration: str
    difficulty_level: str
    variables: Dict[str, Any]
    steps: List[Dict[str, Any]]
    requirements: List[str] = field(default_factory=list)
    comments: List[str] = field(default_factory=list)


class TemplateLibrary:
    """
    Curated library of workflow templates.
    Contains 40+ production-ready templates across 8 categories.
    """
    
    def __init__(self):
        self.templates: Dict[str, WorkflowTemplate] = {}
        self._load_all_templates()
    
    def _load_all_templates(self):
        """Load all templates from all categories"""
        self._load_web_automation_templates()
        self._load_file_management_templates()
        self._load_social_media_templates()
        self._load_office_productivity_templates()
        self._load_system_maintenance_templates()
        self._load_testing_templates()
        self._load_devops_templates()
        self._load_personal_automation_templates()
    
    def get_template(self, template_id: str) -> Optional[WorkflowTemplate]:
        """Get a template by ID"""
        return self.templates.get(template_id)
    
    def get_templates_by_category(self, category: TemplateCategory) -> List[WorkflowTemplate]:
        """Get all templates in a category"""
        return [t for t in self.templates.values() if t.category == category.value]
    
    def get_all_templates(self) -> List[WorkflowTemplate]:
        """Get all templates"""
        return list(self.templates.values())
    
    def search_templates(self, query: str) -> List[WorkflowTemplate]:
        """Search templates by name, description, or tags"""
        query_lower = query.lower()
        results = []
        for t in self.templates.values():
            if (query_lower in t.name.lower() or 
                query_lower in t.description.lower() or
                any(query_lower in tag.lower() for tag in t.tags)):
                results.append(t)
        return results

    # ========== Web Automation Templates ==========
    
    def _load_web_automation_templates(self):
        """Load web automation templates"""
        
        # 1. Login Automation
        login_automation = WorkflowTemplate(
            id="wa_login_001",
            name="Universal Login Automation",
            description="Automated login workflow for websites with username/password authentication, 2FA support, and session management",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.WEB_AUTOMATION.value,
            tags=["login", "authentication", "web", "automation", "2fa"],
            estimated_duration="2-5 minutes",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "url": {"type": "string", "description": "Login page URL", "required": True},
                "username": {"type": "string", "description": "Username or email", "required": True},
                "password": {"type": "string", "description": "Password", "required": True, "sensitive": True},
                "two_fa_code": {"type": "string", "description": "2FA code if enabled", "required": False},
                "remember_me": {"type": "boolean", "description": "Remember login", "default": False},
                "login_button_selector": {"type": "string", "description": "Login button CSS selector", "default": "button[type='submit']"},
                "session_timeout": {"type": "integer", "description": "Session timeout in seconds", "default": 3600}
            },
            steps=[
                {"id": "navigate", "type": "web_navigate", "params": {"url": "${url}", "wait_until": "networkidle"}},
                {"id": "wait_username", "type": "web_wait", "params": {"selector": "input[name='username'], input[type='email']", "timeout": 10}},
                {"id": "fill_username", "type": "web_fill", "params": {"selector": "input[name='username'], input[type='email']", "value": "${username}"}},
                {"id": "fill_password", "type": "web_fill", "params": {"selector": "input[name='password'], input[type='password']", "value": "${password}"}},
                {"id": "toggle_remember", "type": "web_click", "params": {"selector": "input[type='checkbox']", "action": "check"}},
                {"id": "submit_login", "type": "web_click", "params": {"selector": "${login_button_selector}"}},
                {"id": "handle_2fa", "type": "web_conditional", "params": {"condition": "${two_fa_code}", "then": [
                    {"id": "wait_2fa", "type": "web_wait", "params": {"selector": "input[type='tel'], input[name='otp']", "timeout": 5}},
                    {"id": "fill_2fa", "type": "web_fill", "params": {"selector": "input[type='tel'], input[name='otp']", "value": "${two_fa_code}"}},
                    {"id": "submit_2fa", "type": "web_click", "params": {"selector": "button[type='submit']"}}
                ]}},
                {"id": "verify_login", "type": "web_wait", "params": {"selector": "[data-user-profile], .user-menu, .dashboard, #logout", "timeout": 15}},
                {"id": "capture_session", "type": "session_store", "params": {"key": "logged_in_session", "ttl": "${session_timeout}"}}
            ],
            requirements=["Browser automation capability", "Web driver installed"],
            comments=["Supports most standard login forms", "Can be extended for custom OAuth flows"]
        )
        
        # 2. Form Filling Automation
        form_filling = WorkflowTemplate(
            id="wa_form_001",
            name="Smart Form Filler",
            description="Automated form filling workflow with data validation, field detection, and multi-page form support",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.WEB_AUTOMATION.value,
            tags=["form", "web", "automation", "data entry", "scrape"],
            estimated_duration="3-10 minutes",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "form_url": {"type": "string", "description": "Form page URL", "required": True},
                "form_data": {"type": "object", "description": "Key-value pairs for form fields", "required": True},
                "submit_selector": {"type": "string", "description": "Submit button selector", "default": "button[type='submit']"},
                "auto_detect_fields": {"type": "boolean", "description": "Auto-detect form fields", "default": True},
                "wait_between_fields": {"type": "integer", "description": "Delay between fields (ms)", "default": 100},
                "validate_before_submit": {"type": "boolean", "description": "Validate form before submit", "default": True}
            },
            steps=[
                {"id": "navigate", "type": "web_navigate", "params": {"url": "${form_url}"}},
                {"id": "detect_fields", "type": "web_script", "params": {"script": "return Array.from(document.querySelectorAll('input, select, textarea')).map(el => ({name: el.name, id: el.id, type: el.type, tag: el.tagName}))"}},
                {"id": "fill_text_fields", "type": "web_fill_batch", "params": {"data": "${form_data}", "type": "text"}},
                {"id": "fill_selects", "type": "web_fill_batch", "params": {"data": "${form_data}", "type": "select"}},
                {"id": "fill_checkboxes", "type": "web_fill_batch", "params": {"data": "${form_data}", "type": "checkbox"}},
                {"id": "validate", "type": "web_validate", "params": {"required_only": "${validate_before_submit}"}},
                {"id": "submit", "type": "web_click", "params": {"selector": "${submit_selector}"}},
                {"id": "verify", "type": "web_wait", "params": {"selector": ".success, .thank-you, [data-success]", "timeout": 10}}
            ],
            requirements=["Browser automation", "Form field detection capability"],
            comments=["Field matching by name, id, label, and placeholder"]
        )
        
        # 3. Data Scraping Workflow
        data_scraping = WorkflowTemplate(
            id="wa_scrape_001",
            name="Advanced Data Scraper",
            description="Multi-page web scraping with pagination, data extraction, JSON export, and rate limiting",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.WEB_AUTOMATION.value,
            tags=["scraping", "web", "data", "extraction", "crawl"],
            estimated_duration="10-60 minutes",
            difficulty_level=DifficultyLevel.ADVANCED.value,
            variables={
                "start_url": {"type": "string", "description": "Starting URL for scraping", "required": True},
                "selectors": {"type": "object", "description": "CSS selectors for data extraction", "required": True},
                "max_pages": {"type": "integer", "description": "Maximum pages to scrape", "default": 50},
                "pagination_selector": {"type": "string", "description": "Next page button selector", "default": ".pagination a.next"},
                "delay_ms": {"type": "integer", "description": "Delay between requests (ms)", "default": 1000},
                "export_format": {"type": "string", "description": "Export format: json, csv", "default": "json"},
                "output_file": {"type": "string", "description": "Output file path", "default": "./scraped_data.json"}
            },
            steps=[
                {"id": "init", "type": "variable_set", "params": {"var": "page_count", "value": 0}},
                {"id": "navigate_start", "type": "web_navigate", "params": {"url": "${start_url}"}},
                {"id": "extract_data", "type": "web_scrape", "params": {"selectors": "${selectors}", "multiple": True}},
                {"id": "check_pagination", "type": "web_exists", "params": {"selector": "${pagination_selector}"}},
                {"id": "next_page", "type": "web_conditional", "params": {"condition": "${page_count} < ${max_pages}", "then": [
                    {"id": "click_next", "type": "web_click", "params": {"selector": "${pagination_selector}"}},
                    {"id": "increment", "type": "variable_math", "params": {"var": "page_count", "op": "add", "value": 1}},
                    {"id": "delay", "type": "wait", "params": {"ms": "${delay_ms}"}}
                ]}},
                {"id": "loop", "type": "loop", "params": {"condition": "${page_count} < ${max_pages}", "steps": ["extract_data", "check_pagination", "next_page"]}},
                {"id": "export", "type": "file_write", "params": {"path": "${output_file}", "format": "${export_format}"}}
            ],
            requirements=["Web scraping permission", "Respect robots.txt"],
            comments=["Includes rate limiting to avoid IP blocks", "Stores intermediate results"]
        )
        
        # 4. Page Monitoring
        page_monitoring = WorkflowTemplate(
            id="wa_monitor_001",
            name="Page Change Monitor",
            description="Monitor web pages for changes, track updates, send notifications when content changes",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.WEB_AUTOMATION.value,
            tags=["monitoring", "web", "change detection", "watch", "alert"],
            estimated_duration="Continuous (scheduled)",
            difficulty_level=DifficultyLevel.BEGINNER.value,
            variables={
                "urls": {"type": "array", "description": "URLs to monitor", "required": True},
                "check_selector": {"type": "string", "description": "Specific element to monitor", "required": False},
                "check_interval": {"type": "integer", "description": "Check interval in minutes", "default": 60},
                "hash_algorithm": {"type": "string", "description": "Hash for comparison: md5, sha256", "default": "sha256"},
                "notify_on_change": {"type": "boolean", "description": "Send notification on change", "default": True},
                "store_history": {"type": "boolean", "description": "Store change history", "default": True}
            },
            steps=[
                {"id": "load_urls", "type": "variable_set", "params": {"var": "current_url", "value": "${urls}[0]"}},
                {"id": "fetch_page", "type": "web_fetch", "params": {"url": "${current_url}", "selector": "${check_selector}"}},
                {"id": "compute_hash", "type": "hash_compute", "params": {"algorithm": "${hash_algorithm}", "data": "${fetch_page.content}"}},
                {"id": "load_prev_hash", "type": "storage_get", "params": {"key": "prev_hash_${current_url}"}},
                {"id": "compare", "type": "conditional", "params": {"if": "${compute_hash} != ${load_prev_hash}", "then": [
                    {"id": "notify", "type": "notify", "params": {"title": "Page Changed: ${current_url}", "body": "Detected changes on monitored page"}},
                    {"id": "update_hash", "type": "storage_set", "params": {"key": "prev_hash_${current_url}", "value": "${compute_hash}"}},
                    {"id": "log_change", "type": "log", "params": {"message": "Change detected at ${current_url}", "level": "info"}}
                ]}},
                {"id": "update_hash_uncond", "type": "storage_set", "params": {"key": "prev_hash_${current_url}", "value": "${compute_hash}"}}
            ],
            requirements=["Scheduled execution capability", "Notification system"],
            comments=["Uses content hashing for efficient change detection"]
        )
        
        # 5. E-commerce Price Tracker
        price_tracker = WorkflowTemplate(
            id="wa_price_001",
            name="Price Drop Tracker",
            description="Track product prices across e-commerce sites, alert on price drops, log price history",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.WEB_AUTOMATION.value,
            tags=["ecommerce", "price", "tracker", "automation", "deal"],
            estimated_duration="5-15 minutes per check",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "products": {"type": "array", "description": "List of product URLs and info", "required": True},
                "target_price": {"type": "number", "description": "Alert when price below this", "required": False},
                "check_interval": {"type": "integer", "description": "Hours between checks", "default": 24},
                "price_selector": {"type": "string", "description": "CSS selector for price element", "default": "[class*='price'], .product-price"},
                "currency": {"type": "string", "description": "Currency code", "default": "USD"},
                "history_file": {"type": "string", "description": "Price history file", "default": "./price_history.json"}
            },
            steps=[
                {"id": "init", "type": "variable_set", "params": {"var": "product_index", "value": 0}},
                {"id": "get_product", "type": "variable_get", "params": {"var": "products[${product_index}]"}},
                {"id": "navigate", "type": "web_navigate", "params": {"url": "${get_product.url}"}},
                {"id": "extract_price", "type": "web_scrape", "params": {"selectors": {"price": "${price_selector}"}}},
                {"id": "parse_price", "type": "regex_extract", "params": {"pattern": r"[\d,]+\.?\d*", "text": "${extract_price.price}"}},
                {"id": "compare_target", "type": "conditional", "params": {"if": "${parse_price} < ${target_price}", "then": [
                    {"id": "alert", "type": "notify", "params": {"title": "Price Drop! ${get_product.name}", "body": "Now ${parse_price} (target: ${target_price})"}}
                ]}},
                {"id": "save_history", "type": "file_append", "params": {"path": "${history_file}", "data": {"product": "${get_product.name}", "price": "${parse_price}", "timestamp": "${now}"}}},
                {"id": "next", "type": "variable_math", "params": {"var": "product_index", "op": "add", "value": 1}}
            ],
            requirements=["Web automation", "Price parsing logic"],
            comments=["Supports multiple e-commerce site formats"]
        )
        
        self.templates[login_automation.id] = login_automation
        self.templates[form_filling.id] = form_filling
        self.templates[data_scraping.id] = data_scraping
        self.templates[page_monitoring.id] = page_monitoring
        self.templates[price_tracker.id] = price_tracker

    # ========== File Management Templates ==========
    
    def _load_file_management_templates(self):
        """Load file management templates"""
        
        # 6. Backup Automation
        backup_automation = WorkflowTemplate(
            id="fm_backup_001",
            name="Automated Backup System",
            description="Comprehensive backup workflow with compression, encryption, rotation, and remote storage support",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.FILE_MANAGEMENT.value,
            tags=["backup", "automation", "files", "compression", "safety"],
            estimated_duration="10-60 minutes",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "source_paths": {"type": "array", "description": "Paths to backup", "required": True},
                "backup_destination": {"type": "string", "description": "Backup destination path", "required": True},
                "compression": {"type": "string", "description": "Compression: zip, tar.gz, 7z", "default": "tar.gz"},
                "encrypt": {"type": "boolean", "description": "Enable encryption", "default": False},
                "encryption_key": {"type": "string", "description": "Encryption key", "required": False, "sensitive": True},
                "retention_days": {"type": "integer", "description": "Days to keep backups", "default": 30},
                "max_backups": {"type": "integer", "description": "Maximum backups to keep", "default": 10},
                "backup_name_pattern": {"type": "string", "description": "Backup file naming pattern", "default": "backup_{date}_{time}"}
            },
            steps=[
                {"id": "create_timestamp", "type": "datetime_format", "params": {"format": "%Y%m%d_%H%M%S"}},
                {"id": "generate_name", "type": "string_format", "params": {"template": "${backup_name_pattern}", "vars": {"date": "${timestamp}"}}},
                {"id": "scan_files", "type": "file_scan", "params": {"paths": "${source_paths}", "include_hidden": False}},
                {"id": "create_archive", "type": "archive_create", "params": {"format": "${compression}", "source": "${scan_files.files}", "destination": "${backup_destination}/${generate_name}"}},
                {"id": "encrypt_archive", "type": "conditional", "params": {"if": "${encrypt}", "then": [
                    {"id": "encrypt", "type": "encrypt_file", "params": {"algorithm": "AES256", "key": "${encryption_key}", "input": "${create_archive.path}", "output": "${create_archive.path}.enc"}}
                ]}},
                {"id": "verify_backup", "type": "file_verify", "params": {"path": "${create_archive.path}", "checksum": True}},
                {"id": "list_old_backups", "type": "file_list", "params": {"path": "${backup_destination}", "pattern": "*.${compression}", "sort": "modified", "order": "asc"}},
                {"id": "cleanup_old", "type": "file_delete_batch", "params": {"files": "${list_old_backups[0:-${max_backups}]}"}},
                {"id": "log_backup", "type": "log", "params": {"message": "Backup completed: ${generate_name}", "level": "info"}}
            ],
            requirements=["Sufficient disk space", "Encryption tools if enabled"],
            comments=["Supports incremental backups when combined with sync"]
        )
        
        # 7. File Organizer
        file_organizer = WorkflowTemplate(
            id="fm_organize_001",
            name="Smart File Organizer",
            description="Automatically organize files by type, date, size, or custom rules into folder structures",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.FILE_MANAGEMENT.value,
            tags=["files", "organization", "automation", "sort", "clean"],
            estimated_duration="5-30 minutes",
            difficulty_level=DifficultyLevel.BEGINNER.value,
            variables={
                "source_folder": {"type": "string", "description": "Folder to organize", "required": True},
                "organize_by": {"type": "string", "description": "Method: type, date, size, rule", "default": "type"},
                "date_format": {"type": "string", "description": "Date folder format", "default": "%Y/%m"},
                "size_thresholds": {"type": "object", "description": "Size-based thresholds in MB", "default": {"small": 1, "medium": 100, "large": 1000}},
                "dry_run": {"type": "boolean", "description": "Preview without moving", "default": False},
                "create_subfolders": {"type": "boolean", "description": "Create target subfolders", "default": True},
                "conflict_resolution": {"type": "string", "description": "On conflict: rename, skip, overwrite", "default": "rename"}
            },
            steps=[
                {"id": "scan_source", "type": "file_scan", "params": {"path": "${source_folder}", "recursive": True}},
                {"id": "classify_files", "type": "file_classify", "params": {"files": "${scan_source}", "by": "${organize_by}", "rules": {"size_thresholds": "${size_thresholds}", "date_format": "${date_format}"}}},
                {"id": "preview", "type": "file_organize_preview", "params": {"classifications": "${classify_files}"}},
                {"id": "execute_conditional", "type": "conditional", "params": {"if": "not ${dry_run}", "then": [
                    {"id": "create_structure", "type": "folder_create_batch", "params": {"paths": "${classify_files.target_folders}"}},
                    {"id": "move_files", "type": "file_move_batch", "params": {"files": "${classify_files.files}", "destinations": "${classify_files.destinations}", "conflict": "${conflict_resolution}"}}
                ]}},
                {"id": "report", "type": "log", "params": {"message": "Organized ${classify_files.count} files", "level": "info"}}
            ],
            requirements=["Write permissions on source and destination"],
            comments=["Dry run mode for safe testing"]
        )
        
        # 8. Batch Rename
        batch_rename = WorkflowTemplate(
            id="fm_rename_001",
            name="Batch File Renamer",
            description="Rename multiple files using patterns, regex, numbering, date insertion, and case transformation",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.FILE_MANAGEMENT.value,
            tags=["rename", "batch", "files", "automation", "pattern"],
            estimated_duration="1-10 minutes",
            difficulty_level=DifficultyLevel.BEGINNER.value,
            variables={
                "source_pattern": {"type": "string", "description": "Files to rename (glob pattern)", "required": True},
                "source_folder": {"type": "string", "description": "Source folder path", "required": True},
                "rename_pattern": {"type": "string", "description": "New name pattern with placeholders", "required": True},
                "use_regex": {"type": "boolean", "description": "Use regex matching", "default": False},
                "case_transform": {"type": "string", "description": "Case: lower, upper, title, none", "default": "none"},
                "start_number": {"type": "integer", "description": "Starting number for {n}", "default": 1},
                "number_padding": {"type": "integer", "description": "Padding for numbers", "default": 3},
                "preview_only": {"type": "boolean", "description": "Preview changes without renaming", "default": True}
            },
            steps=[
                {"id": "list_files", "type": "file_list", "params": {"path": "${source_folder}", "pattern": "${source_pattern}"}},
                {"id": "sort_files", "type": "list_sort", "params": {"list": "${list_files}", "by": "name", "order": "asc"}},
                {"id": "generate_names", "type": "file_rename_batch", "params": {"files": "${sort_files}", "pattern": "${rename_pattern}", "use_regex": "${use_regex}", "start_num": "${start_number}", "padding": "${number_padding}"}},
                {"id": "preview_changes", "type": "file_rename_preview", "params": {"changes": "${generate_names}"}},
                {"id": "confirm", "type": "user_input", "params": {"prompt": "Proceed with renaming?", "type": "confirm"}},
                {"id": "execute_rename", "type": "conditional", "params": {"if": "${confirm} and not ${preview_only}", "then": [
                    {"id": "rename", "type": "file_rename_execute", "params": {"changes": "${generate_names}"}}
                ]}}
            ],
            requirements=["Write permissions on files"],
            comments=["Placeholders: {name}, {ext}, {n}, {date}, {time}"]
        )
        
        # 9. Format Converter
        format_converter = WorkflowTemplate(
            id="fm_convert_001",
            name="Batch Format Converter",
            description="Convert files between formats with quality control, batch processing, and output optimization",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.FILE_MANAGEMENT.value,
            tags=["convert", "format", "transform", "batch", "automation"],
            estimated_duration="5-60 minutes",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "input_files": {"type": "array", "description": "Files to convert", "required": True},
                "source_format": {"type": "string", "description": "Source format or 'auto'", "default": "auto"},
                "target_format": {"type": "string", "description": "Target format", "required": True},
                "output_folder": {"type": "string", "description": "Output folder", "default": "./converted"},
                "quality": {"type": "integer", "description": "Quality setting 1-100", "default": 85},
                "preserve_metadata": {"type": "boolean", "description": "Keep original metadata", "default": True},
                "overwrite_existing": {"type": "boolean", "description": "Overwrite existing files", "default": False},
                "parallel": {"type": "boolean", "description": "Process in parallel", "default": True},
                "max_workers": {"type": "integer", "description": "Max parallel workers", "default": 4}
            },
            steps=[
                {"id": "detect_formats", "type": "file_detect_format", "params": {"files": "${input_files}"}},
                {"id": "validate_conversion", "type": "format_validate", "params": {"source": "${source_format}", "target": "${target_format}"}},
                {"id": "create_output", "type": "folder_create", "params": {"path": "${output_folder}", "exists_ok": True}},
                {"id": "convert_batch", "type": "file_convert_batch", "params": {"files": "${input_files}", "target": "${target_format}", "output": "${output_folder}", "quality": "${quality}", "metadata": "${preserve_metadata}", "parallel": "${parallel}", "workers": "${max_workers}"}},
                {"id": "verify_output", "type": "file_verify_batch", "params": {"files": "${convert_batch.outputs}"}},
                {"id": "report", "type": "log", "params": {"message": "Converted ${convert_batch.success_count} of ${convert_batch.total} files", "level": "info"}}
            ],
            requirements=["Format-specific conversion tools"],
            comments=["Supports images, documents, audio, video"]
        )
        
        # 10. Duplicate File Finder
        duplicate_finder = WorkflowTemplate(
            id="fm_duplicate_001",
            name="Duplicate File Detector",
            description="Find duplicate files using hash comparison, size matching, or content similarity",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.FILE_MANAGEMENT.value,
            tags=["duplicate", "files", "hash", "cleanup", "dedupe"],
            estimated_duration="10-60 minutes",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "search_paths": {"type": "array", "description": "Folders to search", "required": True},
                "match_method": {"type": "string", "description": "Method: hash, size, name", "default": "hash"},
                "hash_algorithm": {"type": "string", "description": "Hash algorithm: md5, sha256", "default": "sha256"},
                "min_file_size": {"type": "integer", "description": "Minimum file size in bytes", "default": 1024},
                "file_types": {"type": "array", "description": "File extensions to check", "required": False},
                "skip_patterns": {"type": "array", "description": "Glob patterns to skip", "default": ["*.tmp", "*.bak"]},
                "action": {"type": "string", "description": "Action: list, delete, move", "default": "list"},
                "dry_run": {"type": "boolean", "description": "Preview without changes", "default": True}
            },
            steps=[
                {"id": "scan_files", "type": "file_scan", "params": {"paths": "${search_paths}", "min_size": "${min_file_size}", "types": "${file_types}", "exclude": "${skip_patterns}"}},
                {"id": "group_by_size", "type": "file_group", "params": {"files": "${scan_files}", "by": "size"}},
                {"id": "compute_hashes", "type": "hash_compute_batch", "params": {"files": "${group_by_size.groups}", "algorithm": "${hash_algorithm}"}},
                {"id": "find_duplicates", "type": "file_find_duplicates", "params": {"groups": "${compute_hashes}"}},
                {"id": "preview", "type": "file_duplicate_preview", "params": {"duplicates": "${find_duplicates}"}},
                {"id": "execute_action", "type": "conditional", "params": {"if": "not ${dry_run}", "then": [
                    {"id": "delete_duplicates", "type": "file_delete_duplicates", "params": {"duplicates": "${find_duplicates}", "keep": "first", "action": "${action}"}}
                ]}},
                {"id": "report", "type": "log", "params": {"message": "Found ${find_duplicates.count} duplicate groups", "level": "info"}}
            ],
            requirements=["Disk I/O permissions"],
            comments=["Hash-based detection is most accurate"]
        )
        
        self.templates[backup_automation.id] = backup_automation
        self.templates[file_organizer.id] = file_organizer
        self.templates[batch_rename.id] = batch_rename
        self.templates[format_converter.id] = format_converter
        self.templates[duplicate_finder.id] = duplicate_finder

    # ========== Social Media Templates ==========
    
    def _load_social_media_templates(self):
        """Load social media templates"""
        
        # 11. Auto-Poster
        auto_poster = WorkflowTemplate(
            id="sm_post_001",
            name="Social Media Auto-Poster",
            description="Automatically post content to multiple social media platforms with scheduling and analytics",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.SOCIAL_MEDIA.value,
            tags=["social", "posting", "automation", "schedule", "marketing"],
            estimated_duration="1-5 minutes per post",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "content": {"type": "object", "description": "Post content by platform", "required": True},
                "platforms": {"type": "array", "description": "Target platforms", "required": True},
                "media_files": {"type": "array", "description": "Images/videos to attach", "required": False},
                "schedule_time": {"type": "datetime", "description": "When to post (null = immediate)", "required": False},
                "optimize_timing": {"type": "boolean", "description": "Use best time to post", "default": False},
                "hashtags": {"type": "array", "description": "Hashtags to include", "required": False},
                "link_tracking": {"type": "boolean", "description": "Add UTM parameters", "default": True},
                "preview_before_post": {"type": "boolean", "description": "Preview before posting", "default": True}
            },
            steps=[
                {"id": "validate_content", "type": "social_validate", "params": {"content": "${content}", "platforms": "${platforms}"}},
                {"id": "optimize_timing_calc", "type": "conditional", "params": {"if": "${optimize_timing}", "then": [
                    {"id": "analyze_audience", "type": "social_analytics", "params": {"platforms": "${platforms}", "metric": "best_times"}},
                    {"id": "set_schedule", "type": "variable_set", "params": {"var": "schedule_time", "value": "${analyze_audience.optimal_time}"}}
                ]}},
                {"id": "add_tracking", "type": "string_append_utm", "params": {"links": "${content.links}", "source": "${platforms}"}},
                {"id": "prepare_media", "type": "media_optimize", "params": {"files": "${media_files}", "platforms": "${platforms}"}},
                {"id": "create_schedules", "type": "social_schedule_batch", "params": {"content": "${content}", "platforms": "${platforms}", "media": "${prepare_media}", "time": "${schedule_time}"}},
                {"id": "confirm_post", "type": "user_input", "params": {"prompt": "Post to ${platforms}?", "type": "confirm"}},
                {"id": "execute_post", "type": "conditional", "params": {"if": "${confirm_post}", "then": [
                    {"id": "post_batch", "type": "social_post_batch", "params": {"schedules": "${create_schedules}"}}
                ]}}
            ],
            requirements=["Social media API credentials", "Rate limit awareness"],
            comments=["Supports scheduling for future posts"]
        )
        
        # 12. Scheduled Posts Manager
        scheduled_posts = WorkflowTemplate(
            id="sm_schedule_001",
            name="Content Calendar Manager",
            description="Manage and execute scheduled social media posts with calendar view and conflict detection",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.SOCIAL_MEDIA.value,
            tags=["schedule", "calendar", "social", "content", "planning"],
            estimated_duration="5-15 minutes",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "calendar_id": {"type": "string", "description": "Calendar ID to manage", "required": True},
                "posts": {"type": "array", "description": "Scheduled posts", "required": True},
                "timezone": {"type": "string", "description": "Timezone for scheduling", "default": "UTC"},
                "check_conflicts": {"type": "boolean", "description": "Check for posting conflicts", "default": True},
                "auto_adjust": {"type": "boolean", "description": "Auto-adjust conflicting posts", "default": False},
                "reminder_minutes": {"type": "integer", "description": "Minutes before to remind", "default": 60}
            },
            steps=[
                {"id": "fetch_calendar", "type": "social_calendar_fetch", "params": {"calendar_id": "${calendar_id}"}},
                {"id": "parse_posts", "type": "social_calendar_parse", "params": {"posts": "${posts}"}},
                {"id": "check_conflicts_op", "type": "conditional", "params": {"if": "${check_conflicts}", "then": [
                    {"id": "detect_conflicts", "type": "social_conflict_detect", "params": {"existing": "${fetch_calendar}", "new": "${parse_posts}"}},
                    {"id": "resolve_conflicts", "type": "conditional", "params": {"if": "${auto_adjust}", "then": [
                        {"id": "adjust", "type": "social_auto_resolve", "params": {"conflicts": "${detect_conflicts}"}}
                    ]}}
                ]}},
                {"id": "add_to_calendar", "type": "social_calendar_add", "params": {"calendar_id": "${calendar_id}", "posts": "${parse_posts}"}},
                {"id": "set_reminders", "type": "social_reminder_set", "params": {"posts": "${parse_posts}", "before_minutes": "${reminder_minutes}"}},
                {"id": "sync_all", "type": "social_sync_all", "params": {"platforms": "${posts.platforms}"}},
                {"id": "report", "type": "log", "params": {"message": "Scheduled ${parse_posts.count} posts", "level": "info"}}
            ],
            requirements=["Social media API access", "Calendar integration"],
            comments=["Detects content conflicts and duplicates"]
        )
        
        # 13. Content Harvester
        content_harvester = WorkflowTemplate(
            id="sm_harvest_001",
            name="Content Harvester",
            description="Harvest trending content from multiple sources for content curation and research",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.SOCIAL_MEDIA.value,
            tags=["content", "harvest", "curation", "research", "trending"],
            estimated_duration="10-30 minutes",
            difficulty_level=DifficultyLevel.ADVANCED.value,
            variables={
                "sources": {"type": "array", "description": "Content sources to harvest", "required": True},
                "keywords": {"type": "array", "description": "Keywords to track", "required": False},
                "max_per_source": {"type": "integer", "description": "Max items per source", "default": 50},
                "filter_by": {"type": "object", "description": "Filter criteria", "default": {"min_engagement": 100, "language": "en"}},
                "dedupe": {"type": "boolean", "description": "Remove duplicates", "default": True},
                "sentiment_filter": {"type": "string", "description": "Filter: positive, negative, neutral, all", "default": "all"},
                "save_to": {"type": "string", "description": "Output file or database", "default": "./harvested_content.json"},
                "rank_by": {"type": "string", "description": "Ranking: engagement, date, relevance", "default": "engagement"}
            },
            steps=[
                {"id": "init_sources", "type": "variable_set", "params": {"var": "current_source", "value": "${sources}[0]"}},
                {"id": "harvest_source", "type": "social_harvest", "params": {"source": "${current_source}", "keywords": "${keywords}", "max": "${max_per_source}"}},
                {"id": "filter_content", "type": "social_filter", "params": {"items": "${harvest_source}", "criteria": "${filter_by}"}},
                {"id": "analyze_sentiment", "type": "sentiment_analyze_batch", "params": {"items": "${filter_content}"}},
                {"id": "filter_sentiment", "type": "list_filter", "params": {"list": "${analyze_sentiment}", "by": "sentiment", "value": "${sentiment_filter}"}},
                {"id": "dedupe_op", "type": "conditional", "params": {"if": "${dedupe}", "then": [
                    {"id": "dedupe_list", "type": "list_deduplicate", "params": {"list": "${filter_sentiment}", "by": "content"}}
                ]}},
                {"id": "rank", "type": "list_sort", "params": {"list": "${dedupe_op}", "by": "${rank_by}", "order": "desc"}},
                {"id": "save_results", "type": "file_write", "params": {"path": "${save_to}", "data": "${rank}"}},
                {"id": "next_source", "type": "loop_next", "params": {"list": "${sources}", "var": "current_source"}}
            ],
            requirements=["API access to target platforms"],
            comments=["Respect platform terms of service"]
        )
        
        # 14. Engagement Auto-Responder
        engagement_responder = WorkflowTemplate(
            id="sm_engage_001",
            name="Auto Engagement Responder",
            description="Automatically respond to comments, mentions, and messages with AI-generated or templated replies",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.SOCIAL_MEDIA.value,
            tags=["engagement", "auto", "respond", "comments", "automation"],
            estimated_duration="5-20 minutes",
            difficulty_level=DifficultyLevel.ADVANCED.value,
            variables={
                "platforms": {"type": "array", "description": "Platforms to monitor", "required": True},
                "response_mode": {"type": "string", "description": "Mode: template, ai, hybrid", "default": "hybrid"},
                "templates": {"type": "object", "description": "Response templates by type", "required": False},
                "ai_model": {"type": "string", "description": "AI model for responses", "default": "gpt-4"},
                "auto_like": {"type": "boolean", "description": "Auto-like positive mentions", "default": True},
                "escalate_keywords": {"type": "array", "description": "Keywords requiring human review", "default": ["complaint", "refund", "urgent"]},
                "max_responses": {"type": "integer", "description": "Max responses per run", "default": 50},
                "dry_run": {"type": "boolean", "description": "Preview without responding", "default": True}
            },
            steps=[
                {"id": "fetch_mentions", "type": "social_fetch_mentions", "params": {"platforms": "${platforms}", "limit": "${max_responses}"}},
                {"id": "classify_intent", "type": "ai_classify", "params": {"items": "${fetch_mentions}", "model": "${ai_model}"}},
                {"id": "check_escalation", "type": "list_filter", "params": {"list": "${classify_intent}", "by": "contains_keyword", "value": "${escalate_keywords}"}},
                {"id": "escalate", "type": "notify_batch", "params": {"items": "${check_escalation}", "title": "Escalation Required"}},
                {"id": "filter_normal", "type": "list_filter", "params": {"list": "${classify_intent}", "by": "requires_response", "value": True}},
                {"id": "generate_responses", "type": "conditional", "params": {"if": "${response_mode} == 'template'", "then": [
                    {"id": "match_templates", "type": "social_template_match", "params": {"items": "${filter_normal}", "templates": "${templates}"}}
                ], "else": [
                    {"id": "ai_generate", "type": "ai_generate_response", "params": {"items": "${filter_normal}", "model": "${ai_model}", "templates": "${templates}"}}
                ]}},
                {"id": "preview_responses", "type": "social_preview", "params": {"responses": "${generate_responses}"}},
                {"id": "execute_conditional", "type": "conditional", "params": {"if": "not ${dry_run}", "then": [
                    {"id": "post_responses", "type": "social_post_responses", "params": {"responses": "${generate_responses}", "auto_like": "${auto_like}"}}
                ]}}
            ],
            requirements=["Social media API with write access", "AI integration"],
            comments=["Escalation keywords ensure human review for sensitive issues"]
        )
        
        # 15. Hashtag Analyzer
        hashtag_analyzer = WorkflowTemplate(
            id="sm_hashtag_001",
            name="Hashtag Performance Analyzer",
            description="Analyze hashtag performance, find optimal hashtags, track hashtag campaigns",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.SOCIAL_MEDIA.value,
            tags=["hashtag", "analytics", "social", "marketing", "trending"],
            estimated_duration="5-15 minutes",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "hashtags": {"type": "array", "description": "Hashtags to analyze", "required": True},
                "platforms": {"type": "array", "description": "Platforms to analyze on", "required": True},
                "time_range": {"type": "string", "description": "Analysis period: day, week, month", "default": "week"},
                "compare_with": {"type": "array", "description": "Competitor hashtags to compare", "required": False},
                "min_followers": {"type": "integer", "description": "Min follower count", "default": 1000},
                "output_format": {"type": "string", "description": "Output: json, csv, report", "default": "json"}
            },
            steps=[
                {"id": "fetch_metrics", "type": "social_hashtag_metrics", "params": {"hashtags": "${hashtags}", "platforms": "${platforms}", "period": "${time_range}"}},
                {"id": "analyze_performance", "type": "analytics_compute", "params": {"data": "${fetch_metrics}", "metrics": ["reach", "engagement", "posts", "growth"]}},

                {"id": "compare_op", "type": "conditional", "params": {"if": "${compare_with}", "then": [
                    {"id": "fetch_compare", "type": "social_hashtag_metrics", "params": {"hashtags": "${compare_with}", "platforms": "${platforms}", "period": "${time_range}"}},
                    {"id": "comparison_report", "type": "analytics_compare", "params": {"a": "${analyze_performance}", "b": "${fetch_compare}"}}
                ]}},
                {"id": "rank_hashtags", "type": "list_sort", "params": {"list": "${analyze_performance}", "by": "score", "order": "desc"}},
                {"id": "generate_recommendations", "type": "ai_generate", "params": {"prompt": "Based on ${rank_hashtags}, recommend optimal hashtag combinations"}},
                {"id": "export", "type": "file_write", "params": {"path": "./hashtag_analysis.${output_format}", "data": {"hashtags": "${rank_hashtags}", "recommendations": "${generate_recommendations}"}}},
                {"id": "report", "type": "log", "params": {"message": "Analyzed ${hashtags.length} hashtags across ${platforms.length} platforms", "level": "info"}}
            ],
            requirements=["Social media analytics API"],
            comments=["Updates recommendations based on latest trends"]
        )
        
        self.templates[auto_poster.id] = auto_poster
        self.templates[scheduled_posts.id] = scheduled_posts
        self.templates[content_harvester.id] = content_harvester
        self.templates[engagement_responder.id] = engagement_responder
        self.templates[hashtag_analyzer.id] = hashtag_analyzer

    # ========== Office Productivity Templates ==========
    
    def _load_office_productivity_templates(self):
        """Load office productivity templates"""
        
        # 16. Email Auto-Reply
        email_auto_reply = WorkflowTemplate(
            id="op_email_001",
            name="Intelligent Email Auto-Reply",
            description="AI-powered email auto-responder with smart categorization, templates, and priority handling",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.OFFICE_PRODUCTIVITY.value,
            tags=["email", "auto-reply", "automation", "productivity", "ai"],
            estimated_duration="2-5 minutes per email",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "email_account": {"type": "string", "description": "Email account to monitor", "required": True},
                "response_mode": {"type": "string", "description": "Response mode: template, ai, off", "default": "ai"},
                "templates": {"type": "object", "description": "Response templates", "required": False},
                "ai_model": {"type": "string", "description": "AI model for drafting", "default": "gpt-4"},
                "max_daily_replies": {"type": "integer", "description": "Daily reply limit", "default": 100},
                "priority_senders": {"type": "array", "description": "VIP sender emails", "required": False},
                "forward_rules": {"type": "array", "description": "Email forwarding rules", "required": False},
                "respect_signature": {"type": "boolean", "description": "Preserve original signature", "default": True},
                "enable_out_of_office": {"type": "boolean", "description": "Enable OOO replies", "default": False}
            },
            steps=[
                {"id": "fetch_emails", "type": "email_fetch", "params": {"account": "${email_account}", "filter": "unread", "limit": "${max_daily_replies}"}},
                {"id": "classify_emails", "type": "email_classify", "params": {"emails": "${fetch_emails}"}},
                {"id": "check_priority", "type": "email_filter_vip", "params": {"emails": "${classify_emails}", "vip": "${priority_senders}"}},
                {"id": "forward_emails", "type": "conditional", "params": {"if": "${forward_rules}", "then": [
                    {"id": "apply_forward", "type": "email_forward_batch", "params": {"emails": "${classify_emails}", "rules": "${forward_rules}"}}
                ]}},
                {"id": "filter_auto_reply", "type": "email_filter_auto", "params": {"emails": "${classify_emails}", "exclude_vip": True}},
                {"id": "generate_replies", "type": "conditional", "params": {"if": "${response_mode} == 'ai'", "then": [
                    {"id": "ai_draft", "type": "ai_email_draft", "params": {"emails": "${filter_auto_reply}", "model": "${ai_model}", "preserve_sig": "${respect_signature}"}}
                ], "else": [
                    {"id": "template_match", "type": "email_template_match", "params": {"emails": "${filter_auto_reply}", "templates": "${templates}"}}
                ]}},
                {"id": "review_replies", "type": "email_preview", "params": {"drafts": "${generate_replies}"}},
                {"id": "send_replies", "type": "email_send_batch", "params": {"drafts": "${review_replies}"}},
                {"id": "log_summary", "type": "log", "params": {"message": "Processed ${fetch_emails.count} emails, sent ${send_replies.count} replies", "level": "info"}}
            ],
            requirements=["Email API access (IMAP/SMTP)", "AI integration"],
            comments=["VIP senders always get human attention"]
        )
        
        # 17. Document Processor
        document_processor = WorkflowTemplate(
            id="op_doc_001",
            name="Automated Document Processor",
            description="Process documents with OCR, text extraction, formatting, and automated routing",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.OFFICE_PRODUCTIVITY.value,
            tags=["document", "ocr", "automation", "processing", "pdf"],
            estimated_duration="5-20 minutes",
            difficulty_level=DifficultyLevel.ADVANCED.value,
            variables={
                "input_path": {"type": "string", "description": "Folder or file to process", "required": True},
                "output_path": {"type": "string", "description": "Output folder", "required": True},
                "process_types": {"type": "array", "description": "Operations: ocr, extract, convert, combine", "default": ["ocr", "extract"]},
                "ocr_language": {"type": "string", "description": "OCR language", "default": "eng"},
                "extract_content": {"type": "boolean", "description": "Extract text content", "default": True},
                "preserve_formatting": {"type": "boolean", "description": "Keep original formatting", "default": True},
                "convert_to": {"type": "string", "description": "Target format: pdf, docx, txt", "default": "pdf"},
                "combine_mode": {"type": "string", "description": "Combine multiple: merge, concat", "default": "merge"},
                "naming_pattern": {"type": "string", "description": "Output naming pattern", "default": "{original}_processed"}
            },
            steps=[
                {"id": "scan_documents", "type": "file_scan", "params": {"path": "${input_path}", "types": ["pdf", "docx", "doc", "image", "txt"]}},
                {"id": "process_loop", "type": "variable_set", "params": {"var": "current_doc", "value": "${scan_documents}[0]"}},
                {"id": "ocr_process", "type": "conditional", "params": {"if": "'ocr' in ${process_types}", "then": [
                    {"id": "run_ocr", "type": "document_ocr", "params": {"document": "${current_doc}", "language": "${ocr_language}"}}
                ]}},
                {"id": "extract_process", "type": "conditional", "params": {"if": "${extract_content}", "then": [
                    {"id": "extract_text", "type": "document_extract", "params": {"document": "${current_doc}", "formatting": "${preserve_formatting}"}}
                ]}},
                {"id": "convert_process", "type": "conditional", "params": {"if": "'convert' in ${process_types}", "then": [
                    {"id": "convert_doc", "type": "document_convert", "params": {"document": "${current_doc}", "target": "${convert_to}"}}
                ]}},
                {"id": "save_document", "type": "file_write", "params": {"path": "${output_path}/${naming_pattern}.${convert_to}", "data": "${convert_doc.output}"}}},
                {"id": "next_document", "type": "loop_next", "params": {"list": "${scan_documents}", "var": "current_doc"}},
                {"id": "combine_check", "type": "conditional", "params": {"if": "${combine_mode} and ${scan_documents.length} > 1", "then": [
                    {"id": "combine_docs", "type": "document_combine", "params": {"documents": "${scan_documents}", "mode": "${combine_mode}", "output": "${output_path}/combined.${convert_to}"}}
                ]}}
            ],
            requirements=["Document processing libraries", "OCR engine"],
            comments=["Supports batch processing of multiple formats"]
        )
        
        # 18. Spreadsheet Updater
        spreadsheet_updater = WorkflowTemplate(
            id="op_sheet_001",
            name="Smart Spreadsheet Updater",
            description="Automated spreadsheet updates with data validation, formulas, charts, and multi-sheet support",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.OFFICE_PRODUCTIVITY.value,
            tags=["spreadsheet", "excel", "automation", "data", "update"],
            estimated_duration="5-30 minutes",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "spreadsheet_id": {"type": "string", "description": "Spreadsheet file or ID", "required": True},
                "update_type": {"type": "string", "description": "Type: cell, formula, chart, format", "default": "cell"},
                "updates": {"type": "array", "description": "Cell updates to apply", "required": True},
                "sheet_name": {"type": "string", "description": "Target sheet name", "default": "Sheet1"},
                "validate_data": {"type": "boolean", "description": "Validate before updating", "default": True},
                "backup_before": {"type": "boolean", "description": "Backup spreadsheet first", "default": True},
                "notify_on_complete": {"type": "boolean", "description": "Notify when done", "default": True},
                "formula_mode": {"type": "string", "description": "Formula handling: preserve, recalculate", "default": "recalculate"}
            },
            steps=[
                {"id": "load_spreadsheet", "type": "spreadsheet_open", "params": {"id": "${spreadsheet_id}"}},
                {"id": "backup_op", "type": "conditional", "params": {"if": "${backup_before}", "then": [
                    {"id": "create_backup", "type": "file_copy", "params": {"source": "${spreadsheet_id}", "dest": "${spreadsheet_id}.backup.${timestamp}"}}
                ]}},
                {"id": "validate_updates", "type": "spreadsheet_validate", "params": {"updates": "${updates}", "sheet": "${sheet_name}"}},
                {"id": "apply_updates", "type": "spreadsheet_update_batch", "params": {"spreadsheet": "${load_spreadsheet}", "updates": "${validate_updates}", "sheet": "${sheet_name}"}}},
                {"id": "recalculate_formulas", "type": "conditional", "params": {"if": "${formula_mode} == 'recalculate'", "then": [
                    {"id": "calc", "type": "spreadsheet_recalculate", "params": {"spreadsheet": "${load_spreadsheet}"}}
                ]}},
                {"id": "refresh_charts", "type": "conditional", "params": {"if": "'chart' in ${update_type}", "then": [
                    {"id": "update_charts", "type": "spreadsheet_refresh_charts", "params": {"spreadsheet": "${load_spreadsheet}"}}
                ]}},
                {"id": "save_spreadsheet", "type": "spreadsheet_save", "params": {"spreadsheet": "${load_spreadsheet}"}},
                {"id": "notify_complete", "type": "conditional", "params": {"if": "${notify_on_complete}", "then": [
                    {"id": "send_notification", "type": "notify", "params": {"title": "Spreadsheet Updated", "body": "Applied ${updates.length} updates to ${spreadsheet_id}"}}
                ]}}
            ],
            requirements=["Spreadsheet library (openpyxl, pandas)"],
            comments=["Supports Google Sheets and local Excel files"]
        )
        
        # 19. Meeting Notes Generator
        meeting_notes = WorkflowTemplate(
            id="op_meeting_001",
            name="Automated Meeting Notes",
            description="Generate meeting notes from transcripts, extract action items, and distribute to participants",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.OFFICE_PRODUCTIVITY.value,
            tags=["meeting", "notes", "transcript", "automation", "summary"],
            estimated_duration="5-15 minutes",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "transcript_source": {"type": "string", "description": "Transcript file or recording", "required": True},
                "meeting_title": {"type": "string", "description": "Meeting title", "required": True},
                "attendees": {"type": "array", "description": "Attendee emails", "required": False},
                "extract_action_items": {"type": "boolean", "description": "Extract action items", "default": True},
                "summarize_key_points": {"type": "boolean", "description": "Generate key points summary", "default": True},
                "template": {"type": "string", "description": "Notes template", "default": "standard"},
                "output_format": {"type": "string", "description": "Format: docx, pdf, md", "default": "docx"},
                "distribute": {"type": "boolean", "description": "Email to attendees", "default": True}
            },
            steps=[
                {"id": "load_transcript", "type": "file_read", "params": {"path": "${transcript_source}"}},
                {"id": "parse_speakers", "type": "ai_parse_speakers", "params": {"transcript": "${load_transcript}"}},
                {"id": "extract_decisions", "type": "ai_extract", "params": {"text": "${load_transcript}", "type": "decisions"}},
                {"id": "extract_actions", "type": "conditional", "params": {"if": "${extract_action_items}", "then": [
                    {"id": "get_actions", "type": "ai_extract", "params": {"text": "${load_transcript}", "type": "action_items"}}
                ]}},
                {"id": "summarize_key", "type": "conditional", "params": {"if": "${summarize_key_points}", "then": [
                    {"id": "get_summary", "type": "ai_summarize", "params": {"text": "${load_transcript}", "max_points": 5}}
                ]}},
                {"id": "generate_notes", "type": "document_generate", "params": {"template": "${template}", "title": "${meeting_title}", "attendees": "${attendees}", "transcript": "${load_transcript}", "decisions": "${extract_decisions}", "actions": "${extract_actions}", "summary": "${summarize_key}"}}},
                {"id": "format_notes", "type": "document_convert", "params": {"document": "${generate_notes}", "target": "${output_format}"}},
                {"id": "save_notes", "type": "file_write", "params": {"path": "./meeting_notes/${meeting_title}.${output_format}", "data": "${format_notes}"}}},
                {"id": "distribute_notes", "type": "conditional", "params": {"if": "${distribute}", "then": [
                    {"id": "send_email", "type": "email_send", "params": {"to": "${attendees}", "subject": "Notes: ${meeting_title}", "body": "${generate_notes}", "attachments": ["${format_notes}"]}}
                ]}}
            ],
            requirements=["AI transcription service", "Document generation"],
            comments=["Action items are assigned to attendees when emails available"]
        )
        
        # 20. Report Generator
        report_generator = WorkflowTemplate(
            id="op_report_001",
            name="Automated Report Generator",
            description="Generate periodic reports from multiple data sources with charts, tables, and auto-distribution",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.OFFICE_PRODUCTIVITY.value,
            tags=["report", "automation", "analytics", "dashboard", "generation"],
            estimated_duration="15-60 minutes",
            difficulty_level=DifficultyLevel.ADVANCED.value,
            variables={
                "report_type": {"type": "string", "description": "Report type: daily, weekly, monthly, custom", "required": True},
                "data_sources": {"type": "array", "description": "Data source configurations", "required": True},
                "template_id": {"type": "string", "description": "Report template ID", "required": True},
                "output_format": {"type": "string", "description": "Output: pdf, html, pptx", "default": "pdf"},
                "include_charts": {"type": "boolean", "description": "Include charts", "default": True},
                "include_raw_data": {"type": "boolean", "description": "Include data tables", "default": True},
                "recipients": {"type": "array", "description": "Email recipients", "required": False},
                "publish_url": {"type": "string", "description": "URL to publish report", "required": False},
                "compare_period": {"type": "boolean", "description": "Include period comparison", "default": True}
            },
            steps=[
                {"id": "fetch_data", "type": "data_fetch_batch", "params": {"sources": "${data_sources}", "period": "${report_type}"}}},
                {"id": "process_data", "type": "data_process", "params": {"data": "${fetch_data}", "operations": ["clean", "aggregate", "calculate"]}}},
                {"id": "compare_data", "type": "conditional", "params": {"if": "${compare_period}", "then": [
                    {"id": "fetch_prev", "type": "data_fetch_batch", "params": {"sources": "${data_sources}", "period": "previous_${report_type}"}}},
                    {"id": "calculate_change", "type": "data_compare", "params": {"current": "${process_data}", "previous": "${fetch_prev}"}}
                ]}},
                {"id": "generate_charts", "type": "conditional", "params": {"if": "${include_charts}", "then": [
                    {"id": "create_charts", "type": "chart_generate_batch", "params": {"data": "${process_data}", "chart_types": ["line", "bar", "pie"]}}
                ]}},
                {"id": "create_tables", "type": "conditional", "params": {"if": "${include_raw_data}", "then": [
                    {"id": "format_tables", "type": "table_format", "params": {"data": "${process_data}", "style": "professional"}}
                ]}},
                {"id": "render_report", "type": "report_render", "params": {"template": "${template_id}", "data": "${process_data}", "charts": "${create_charts}", "tables": "${format_tables}", "comparison": "${calculate_change}"}}},
                {"id": "export_report", "type": "report_export", "params": {"report": "${render_report}", "format": "${output_format}"}},
                {"id": "distribute_report", "type": "conditional", "params": {"if": "${recipients}", "then": [
                    {"id": "email_report", "type": "email_send", "params": {"to": "${recipients}", "subject": "${report_type} Report - ${date}", "attachments": ["${export_report}"]}}
                ]}},
                {"id": "publish_report", "type": "conditional", "params": {"if": "${publish_url}", "then": [
                    {"id": "upload_report", "type": "file_upload", "params": {"file": "${export_report}", "url": "${publish_url}"}}
                ]}}
            ],
            requirements=["Data source connections", "Report template"],
            comments=["Fully automated when scheduled"]
        )
        
        self.templates[email_auto_reply.id] = email_auto_reply
        self.templates[document_processor.id] = document_processor
        self.templates[spreadsheet_updater.id] = spreadsheet_updater
        self.templates[meeting_notes.id] = meeting_notes
        self.templates[report_generator.id] = report_generator

    # ========== System Maintenance Templates ==========
    
    def _load_system_maintenance_templates(self):
        """Load system maintenance templates"""
        
        # 21. Disk Space Manager
        disk_manager = WorkflowTemplate(
            id="sm_disk_001",
            name="Intelligent Disk Space Manager",
            description="Monitor and manage disk space with automated cleanup, alerting, and large file detection",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.SYSTEM_MAINTENANCE.value,
            tags=["disk", "storage", "cleanup", "maintenance", "space"],
            estimated_duration="10-30 minutes",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "paths_to_monitor": {"type": "array", "description": "Paths to analyze", "required": True},
                "warning_threshold": {"type": "integer", "description": "Warning at % used", "default": 80},
                "critical_threshold": {"type": "integer", "description": "Critical at % used", "default": 90},
                "large_file_size": {"type": "integer", "description": "Large file size in MB", "default": 100},
                "cleanup_patterns": {"type": "array", "description": "File patterns to clean", "default": ["*.tmp", "*.log", "__pycache__", ".cache"]},
                "dry_run": {"type": "boolean", "description": "Preview without deleting", "default": True},
                "auto_cleanup": {"type": "boolean", "description": "Auto-clean safe patterns", "default": False},
                "exclude_paths": {"type": "array", "description": "Paths to exclude", "default": ["/system", "/proc"]}
            },
            steps=[
                {"id": "check_disk_usage", "type": "system_disk_usage", "params": {"paths": "${paths_to_monitor}"}},
                {"id": "check_thresholds", "type": "system_check_threshold", "params": {"usage": "${check_disk_usage}", "warning": "${warning_threshold}", "critical": "${critical_threshold}"}}},
                {"id": "alert_critical", "type": "conditional", "params": {"if": "${check_thresholds.critical}", "then": [
                    {"id": "send_alert", "type": "notify", "params": {"title": "Critical: Low Disk Space", "body": "${check_disk_usage}", "priority": "high"}}
                ]}},
                {"id": "alert_warning", "type": "conditional", "params": {"if": "${check_thresholds.warning}", "then": [
                    {"id": "send_warning", "type": "notify", "params": {"title": "Warning: Disk Space Low", "body": "${check_disk_usage}"}}
                ]}},
                {"id": "find_large_files", "type": "file_find_large", "params": {"paths": "${paths_to_monitor}", "min_size_mb": "${large_file_size}", "exclude": "${exclude_paths}"}}},
                {"id": "scan_cleanup", "type": "file_scan_patterns", "params": {"paths": "${paths_to_monitor}", "patterns": "${cleanup_patterns}"}}},
                {"id": "preview_cleanup", "type": "file_cleanup_preview", "params": {"files": "${scan_cleanup}", "exclude": "${exclude_paths}"}}},
                {"id": "execute_cleanup", "type": "conditional", "params": {"if": "not ${dry_run} and ${auto_cleanup}", "then": [
                    {"id": "delete_files", "type": "file_delete_batch", "params": {"files": "${preview_cleanup.safe_to_delete}"}}
                ]}},
                {"id": "report", "type": "log", "params": {"message": "Disk analysis: ${check_disk_usage.total_used_gb}GB used of ${check_disk_usage.total_gb}GB", "level": "info"}}
            ],
            requirements=["File system access", "Admin privileges for some operations"],
            comments=["Never deletes from system-protected paths"]
        )
        
        # 22. Cache Cleaner
        cache_cleaner = WorkflowTemplate(
            id="sm_cache_001",
            name="Multi-Platform Cache Cleaner",
            description="Clean cache from browsers, applications, and system with safe validation",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.SYSTEM_MAINTENANCE.value,
            tags=["cache", "cleanup", "browser", "system", "maintenance"],
            estimated_duration="5-15 minutes",
            difficulty_level=DifficultyLevel.BEGINNER.value,
            variables={
                "clean_targets": {"type": "array", "description": "Targets: browser, app, system, all", "default": ["browser", "app"]},
                "browsers": {"type": "array", "description": "Browsers to clean", "default": ["chrome", "firefox", "safari"]},
                "app_cache_dirs": {"type": "array", "description": "App-specific cache dirs", "required": False},
                "preserve_cookies": {"type": "boolean", "description": "Keep session cookies", "default": True},
                "preserve_bookmarks": {"type": "boolean", "description": "Keep bookmarks", "default": True},
                "min_age_hours": {"type": "integer", "description": "Only clean files older than (hours)", "default": 24},
                "dry_run": {"type": "boolean", "description": "Preview without deleting", "default": True},
                "calculate_space": {"type": "boolean", "description": "Calculate freed space", "default": True}
            },
            steps=[
                {"id": "init_targets", "type": "variable_set", "params": {"var": "current_target", "value": "${clean_targets}[0]"}},
                {"id": "scan_browser", "type": "conditional", "params": {"if": "'browser' in ${clean_targets}", "then": [
                    {"id": "detect_browsers", "type": "browser_detect", "params": {"browsers": "${browsers}"}}},
                    {"id": "scan_browser_cache", "type": "browser_cache_scan", "params": {"browsers": "${detect_browsers}", "min_age": "${min_age_hours}"}}
                ]}},
                {"id": "scan_app_cache", "type": "conditional", "params": {"if": "'app' in ${clean_targets}", "then": [
                    {"id": "scan_app", "type": "app_cache_scan", "params": {"apps": "${app_cache_dirs}"}}}
                ]}},
                {"id": "scan_system_cache", "type": "conditional", "params": {"if": "'system' in ${clean_targets}", "then": [
                    {"id": "scan_sys", "type": "system_cache_scan", "params": {"}}
                ]}},
                {"id": "aggregate_caches", "type": "list_concat", "params": {"lists": ["${scan_browser_cache}", "${scan_app_cache}", "${scan_sys_cache}"]}}},
                {"id": "filter_protected", "type": "cache_filter_protected", "params": {"items": "${aggregate_caches}", "preserve_cookies": "${preserve_cookies}", "preserve_bookmarks": "${preserve_bookmarks}"}}},
                {"id": "preview_clean", "type": "cache_preview", "params": {"items": "${filter_protected}"}},
                {"id": "execute_clean", "type": "conditional", "params": {"if": "not ${dry_run}", "then": [
                    {"id": "clean", "type": "cache_clean_batch", "params": {"items": "${preview_clean}"}}
                ]}},
                {"id": "report_space", "type": "conditional", "params": {"if": "${calculate_space}", "then": [
                    {"id": "calc_space", "type": "system_calculate_freed", "params": {"cleaned": "${execute_clean}"}},
                    {"id": "log_space", "type": "log", "params": {"message": "Freed ${calc_space}MB of disk space", "level": "info"}}
                ]}}
            ],
            requirements=["File system access"],
            comments=["Safe mode protects important data"]
        )
        
        # 23. Log Rotator
        log_rotator = WorkflowTemplate(
            id="sm_log_001",
            name="Automated Log Rotator",
            description="Rotate, compress, archive, and clean log files with configurable retention policies",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.SYSTEM_MAINTENANCE.value,
            tags=["log", "rotation", "archive", "maintenance", "compression"],
            estimated_duration="5-20 minutes",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "log_directories": {"type": "array", "description": "Directories with logs", "required": True},
                "max_size_mb": {"type": "integer", "description": "Rotate when file exceeds (MB)", "default": 100},
                "max_files": {"type": "integer", "description": "Max rotated files to keep", "default": 10},
                "retention_days": {"type": "integer", "description": "Days to keep logs", "default": 30},
                "compression": {"type": "string", "description": "Compression: gzip, bzip2, xz", "default": "gzip"},
                "archive_path": {"type": "string", "description": "Archive destination", "default": "./log_archive"},
                "timestamp_format": {"type": "string", "description": "Rotated file naming", "default": "%Y%m%d_%H%M%S"},
                "copy_truncate": {"type": "boolean", "description": "Use copy-truncate method", "default": True},
                "create_summary": {"type": "boolean", "description": "Create rotation summary", "default": True}
            },
            steps=[
                {"id": "scan_logs", "type": "file_scan", "params": {"paths": "${log_directories}", "types": ["log", "txt"], "recursive": True}},
                {"id": "check_sizes", "type": "file_check_size", "params": {"files": "${scan_logs}", "threshold_mb": "${max_size_mb}"}}},
                {"id": "rotate_files", "type": "log_rotate_batch", "params": {"files": "${check_sizes.needs_rotation}", "max_files": "${max_files}", "timestamp": "${timestamp_format}", "copy_truncate": "${copy_truncate}"}}},
                {"id": "compress_rotated", "type": "archive_compress_batch", "params": {"files": "${rotate_files.rotated}", "format": "${compression}"}}},
                {"id": "move_archives", "type": "file_move_batch", "params": {"files": "${compress_rotated}", "destination": "${archive_path}"}}},
                {"id": "check_retention", "type": "file_age_check", "params": {"path": "${archive_path}", "max_age_days": "${retention_days}"}}},
                {"id": "delete_old", "type": "file_delete_batch", "params": {"files": "${check_retention.old_files}"}},
                {"id": "create_summary_op", "type": "conditional", "params": {"if": "${create_summary}", "then": [
                    {"id": "write_summary", "type": "file_write", "params": {"path": "${archive_path}/rotation_summary_${timestamp}.log", "data": {"rotated": "${rotate_files.count}", "compressed": "${compress_rotated.count}", "deleted": "${delete_old.count}"}}}
                ]}},
                {"id": "report", "type": "log", "params": {"message": "Log rotation complete: ${rotate_files.count} files rotated, ${delete_old.count} deleted", "level": "info"}}
            ],
            requirements=["Write access to log directories"],
            comments=["Copy-truncate is safer for active logs"]
        )
        
        # 24. System Health Monitor
        system_health = WorkflowTemplate(
            id="sm_health_001",
            name="System Health Monitor",
            description="Monitor CPU, memory, processes, and services with alerting and trend analysis",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.SYSTEM_MAINTENANCE.value,
            tags=["health", "monitor", "system", "metrics", "alert"],
            estimated_duration="1-5 minutes",
            difficulty_level=DifficultyLevel.BEGINNER.value,
            variables={
                "check_interval": {"type": "integer", "description": "Check interval in minutes", "default": 5},
                "cpu_threshold": {"type": "integer", "description": "CPU alert threshold %", "default": 90},
                "memory_threshold": {"type": "integer", "description": "Memory alert threshold %", "default": 85},
                "disk_threshold": {"type": "integer", "description": "Disk alert threshold %", "default": 90},
                "watched_processes": {"type": "array", "description": "Processes to monitor", "required": False},
                "watched_services": {"type": "array", "description": "Services to check", "required": False},
                "alert_channels": {"type": "array", "description": "Alert methods: log, email, notify", "default": ["log", "notify"]},
                "store_history": {"type": "boolean", "description": "Store metrics history", "default": True},
                "history_days": {"type": "integer", "description": "Days of history to keep", "default": 30}
            },
            steps=[
                {"id": "check_cpu", "type": "system_cpu_usage", "params": {}},
                {"id": "check_memory", "type": "system_memory_usage", "params": {}},
                {"id": "check_disk", "type": "system_disk_usage", "params": {}},
                {"id": "check_processes", "type": "conditional", "params": {"if": "${watched_processes}", "then": [
                    {"id": "monitor_processes", "type": "system_process_check", "params": {"processes": "${watched_processes}"}}
                ]}},
                {"id": "check_services", "type": "conditional", "params": {"if": "${watched_services}", "then": [
                    {"id": "monitor_services", "type": "system_service_check", "params": {"services": "${watched_services}"}}
                ]}},
                {"id": "analyze_metrics", "type": "system_analyze", "params": {"cpu": "${check_cpu}", "memory": "${check_memory}", "disk": "${check_disk}"}}},
                {"id": "check_thresholds", "type": "system_check_threshold", "params": {"cpu": "${check_cpu}", "memory": "${check_memory}", "disk": "${check_disk}", "thresholds": {"cpu": "${cpu_threshold}", "memory": "${memory_threshold}", "disk": "${disk_threshold}"}}}},
                {"id": "alert_issues", "type": "conditional", "params": {"if": "${check_thresholds.has_issues}", "then": [
                    {"id": "send_alerts", "type": "alert_batch", "params": {"channels": "${alert_channels}", "issues": "${check_thresholds.issues}"}}
                ]}},
                {"id": "store_metrics", "type": "conditional", "params": {"if": "${store_history}", "then": [
                    {"id": "save_history", "type": "storage_append", "params": {"key": "system_health_history", "value": "${analyze_metrics}", "ttl_days": "${history_days}"}}
                ]}}
            ],
            requirements=["System monitoring tools"],
            comments=["Suitable for continuous monitoring when scheduled"]
        )
        
        # 25. Startup Manager
        startup_manager = WorkflowTemplate(
            id="sm_startup_001",
            name="Startup Program Manager",
            description="Manage startup applications, disable unnecessary programs, optimize boot time",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.SYSTEM_MAINTENANCE.value,
            tags=["startup", "boot", "optimization", "programs", "windows", "macos"],
            estimated_duration="5-10 minutes",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "action": {"type": "string", "description": "Action: list, disable, enable, optimize", "default": "list"},
                "target_programs": {"type": "array", "description": "Programs to modify", "required": False},
                "safe_mode": {"type": "boolean", "description": "Safe mode (skip critical)", "default": True},
                "critical_patterns": {"type": "array", "description": "Programs to never disable", "default": ["antivirus", "security", "driver"]},
                "measure_boot_time": {"type": "boolean", "description": "Measure boot time impact", "default": True},
                "create_restore_point": {"type": "boolean", "description": "Create restore point before", "default": True}
            },
            steps=[
                {"id": "list_startup", "type": "system_startup_list", "params": {}},
                {"id": "categorize", "type": "system_startup_categorize", "params": {"items": "${list_startup}"}},
                {"id": "check_safe", "type": "conditional", "params": {"if": "${safe_mode}", "then": [
                    {"id": "mark_critical", "type": "system_mark_critical", "params": {"items": "${categorize}", "patterns": "${critical_patterns}"}}
                ]}},
                {"id": "disable_action", "type": "conditional", "params": {"if": "${action} == 'disable'", "then": [
                    {"id": "filter_disable", "type": "list_filter", "params": {"list": "${target_programs}", "exclude": "${mark_critical.critical}"}}},
                    {"id": "create_restore", "type": "system_create_restore", "params": {"}},
                    {"id": "disable_batch", "type": "system_startup_disable", "params": {"programs": "${filter_disable}"}}
                ]}},
                {"id": "enable_action", "type": "conditional", "params": {"if": "${action} == 'enable'", "then": [
                    {"id": "enable_batch", "type": "system_startup_enable", "params": {"programs": "${target_programs}"}}
                ]}},
                {"id": "optimize_action", "type": "conditional", "params": {"if": "${action} == 'optimize'", "then": [
                    {"id": "analyze_impact", "type": "system_startup_analyze", "params": {"items": "${list_startup}"}}},
                    {"id": "recommend_disable", "type": "system_recommend_disable", "params": {"analysis": "${analyze_impact}"}}},
                    {"id": "apply_recommendations", "type": "system_startup_disable", "params": {"programs": "${recommend_disable}"}}
                ]}},
                {"id": "measure_boot", "type": "conditional", "params": {"if": "${measure_boot_time} and ${action} != 'list'", "then": [
                    {"id": "time_boot", "type": "system_measure_boot", "params": {}},
                    {"id": "log_improvement", "type": "log", "params": {"message": "Boot time: ${time_boot.before} -> ${time_boot.after}", "level": "info"}}
                ]}},
                {"id": "report_final", "type": "log", "params": {"message": "Startup management: ${action} completed for ${list_startup.count} items", "level": "info"}}
            ],
            requirements=["Admin/root privileges"],
            comments=["Safe mode protects critical system programs"]
        )
        
        self.templates[disk_manager.id] = disk_manager
        self.templates[cache_cleaner.id] = cache_cleaner
        self.templates[log_rotator.id] = log_rotator
        self.templates[system_health.id] = system_health
        self.templates[startup_manager.id] = startup_manager

    # ========== Testing Templates ==========
    
    def _load_testing_templates(self):
        """Load testing templates"""
        
        # 26. App Smoke Test
        smoke_test = WorkflowTemplate(
            id="test_smoke_001",
            name="Application Smoke Test",
            description="Quick smoke testing suite for app launch, core features, and basic functionality",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.TESTING.value,
            tags=["testing", "smoke", "app", "automation", "qa"],
            estimated_duration="10-30 minutes",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "app_path": {"type": "string", "description": "Application path or URL", "required": True},
                "app_type": {"type": "string", "description": "Type: web, desktop, mobile", "default": "web"},
                "test_cases": {"type": "array", "description": "Specific test cases to run", "required": False},
                "critical_paths": {"type": "array", "description": "Critical user journeys", "default": ["login", "main_dashboard", "core_feature"]},
                "screenshot_on_fail": {"type": "boolean", "description": "Capture screenshot on failure", "default": True},
                "headless": {"type": "boolean", "description": "Run headless (web)", "default": False},
                "timeout_seconds": {"type": "integer", "description": "Step timeout", "default": 30},
                "report_format": {"type": "string", "description": "Report: json, html, xml", "default": "json"}
            },
            steps=[
                {"id": "prepare_app", "type": "test_prepare", "params": {"app": "${app_path}", "type": "${app_type}", "headless": "${headless}"}}},
                {"id": "test_launch", "type": "test_execute", "params": {"name": "App Launch", "action": "launch", "expected": "app_opened", "timeout": "${timeout_seconds}"}}},
                {"id": "test_critical_paths", "type": "test_execute_batch", "params": {"tests": "${critical_paths}", "type": "${app_type}"}}},
                {"id": "capture_failures", "type": "conditional", "params": {"if": "${test_critical_paths.has_failures} and ${screenshot_on_fail}", "then": [
                    {"id": "screenshots", "type": "test_capture_screenshots", "params": {"failures": "${test_critical_paths.failures}"}}
                ]}},
                {"id": "generate_report", "type": "test_report", "params": {"results": "${test_critical_paths}", "format": "${report_format}"}}},
                {"id": "cleanup", "type": "test_cleanup", "params": {"app": "${app_path}"}},
                {"id": "exit_status", "type": "test_exit", "params": {"code": "${test_critical_paths.all_passed}"}}
            ],
            requirements=["Testing framework", "Application automation tools"],
            comments=["Critical paths must all pass for smoke test to pass"]
        )
        
        # 27. Screenshot Diff
        screenshot_diff = WorkflowTemplate(
            id="test_screenshot_001",
            name="Visual Screenshot Diff",
            description="Compare screenshots for visual regression testing with pixel diff and similarity scoring",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.TESTING.value,
            tags=["screenshot", "visual", "diff", "regression", "testing"],
            estimated_duration="5-15 minutes",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "baseline_dir": {"type": "string", "description": "Baseline screenshots folder", "required": True},
                "test_dir": {"type": "string", "description": "New screenshots to test", "required": True},
                "output_dir": {"type": "string", "description": "Diff output folder", "default": "./diff_results"},
                "threshold": {"type": "number", "description": "Similarity threshold 0-1", "default": 0.95},
                "diff_method": {"type": "string", "description": "Method: pixel, perceptual, layout", "default": "pixel"},
                "highlight_changes": {"type": "boolean", "description": "Highlight differences in output", "default": True},
                "auto_update_baseline": {"type": "boolean", "description": "Auto-update if within tolerance", "default": False},
                "report_format": {"type": "string", "description": "Report format", "default": "html"}
            },
            steps=[
                {"id": "list_baseline", "type": "file_list", "params": {"path": "${baseline_dir}", "types": ["png", "jpg", "jpeg"]}},
                {"id": "list_test", "type": "file_list", "params": {"path": "${test_dir}", "types": ["png", "jpg", "jpeg"]}},
                {"id": "match_pairs", "type": "test_match_screenshots", "params": {"baseline": "${list_baseline}", "test": "${list_test}"}}},
                {"id": "compare_pair", "type": "variable_set", "params": {"var": "current_pair", "value": "${match_pairs}[0]"}},
                {"id": "pixel_diff", "type": "image_diff", "params": {"baseline": "${current_pair.baseline}", "test": "${current_pair.test}", "method": "${diff_method}"}}},
                {"id": "calculate_similarity", "type": "image_similarity", "params": {"image1": "${current_pair.baseline}", "image2": "${current_pair.test}"}}},
                {"id": "check_threshold", "type": "conditional", "params": {"if": "${calculate_similarity} < ${threshold}", "then": [
                    {"id": "highlight_diff", "type": "image_highlight", "params": {"diff": "${pixel_diff}", "output": "${output_dir}/${current_pair.name}_diff.png"}},
                    {"id": "mark_failed", "type": "test_mark_failed", "params": {"test": "${current_pair.name}", "similarity": "${calculate_similarity}"}}
                ], "else": [
                    {"id": "mark_passed", "type": "test_mark_passed", "params": {"test": "${current_pair.name}"}},
                    {"id": "auto_update", "type": "conditional", "params": {"if": "${auto_update_baseline}", "then": [
                        {"id": "update_baseline", "type": "file_copy", "params": {"source": "${current_pair.test}", "dest": "${baseline_dir}/${current_pair.name}"}}
                    ]}}
                ]}},
                {"id": "next_pair", "type": "loop_next", "params": {"list": "${match_pairs}", "var": "current_pair"}},
                {"id": "generate_report", "type": "test_diff_report", "params": {"results": "${match_pairs}", "output": "${output_dir}", "format": "${report_format}"}}
            ],
            requirements=["Image processing libraries (Pillow, OpenCV)"],
            comments=["Perceptual diff handles anti-aliasing better"]
        )
        
        # 28. Regression Tester
        regression_tester = WorkflowTemplate(
            id="test_regress_001",
            name="Comprehensive Regression Tester",
            description="Full regression test suite with test case management, parallel execution, and detailed reporting",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.TESTING.value,
            tags=["testing", "regression", "qa", "automation", "suite"],
            estimated_duration="30-120 minutes",
            difficulty_level=DifficultyLevel.ADVANCED.value,
            variables={
                "test_suite_path": {"type": "string", "description": "Test suite definition file", "required": True},
                "test_environment": {"type": "string", "description": "Environment: dev, staging, prod", "default": "staging"},
                "parallel_workers": {"type": "integer", "description": "Parallel test workers", "default": 4},
                "rerun_failed": {"type": "boolean", "description": "Rerun failed tests once", "default": True},
                "screenshot_mode": {"type": "string", "description": "Mode: all, failed, none", "default": "failed"},
                "test_timeout": {"type": "integer", "description": "Test timeout in seconds", "default": 60},
                "data_profile": {"type": "string", "description": "Test data profile", "default": "default"},
                "report_detail": {"type": "string", "description": "Detail level: minimal, standard, verbose", "default": "standard"}
            },
            steps=[
                {"id": "load_suite", "type": "test_load_suite", "params": {"path": "${test_suite_path}"}},
                {"id": "setup_env", "type": "test_setup_env", "params": {"environment": "${test_environment}", "profile": "${data_profile}"}}},
                {"id": "parse_tests", "type": "test_parse", "params": {"suite": "${load_suite}"}},
                {"id": "group_tests", "type": "test_group", "params": {"tests": "${parse_tests}", "by": "priority", "parallel": "${parallel_workers}"}}},
                {"id": "execute_priority", "type": "test_execute_batch", "params": {"tests": "${group_tests.critical}", "parallel": 1, "timeout": "${test_timeout}", "screenshot": "${screenshot_mode}"}}},
                {"id": "execute_parallel", "type": "test_execute_batch", "params": {"tests": "${group_tests.high}", "parallel": "${parallel_workers}", "timeout": "${test_timeout}", "screenshot": "${screenshot_mode}"}}},
                {"id": "execute_remaining", "type": "test_execute_batch", "params": {"tests": "${group_tests.medium}", "parallel": "${parallel_workers}", "timeout": "${test_timeout}", "screenshot": "${screenshot_mode}"}}},
                {"id": "aggregate_results", "type": "test_aggregate", "params": {"results": ["${execute_priority}", "${execute_parallel}", "${execute_remaining}"]}}},
                {"id": "rerun_check", "type": "conditional", "params": {"if": "${rerun_failed} and ${aggregate_results.has_failures}", "then": [
                    {"id": "rerun", "type": "test_rerun_failed", "params": {"results": "${aggregate_results}", "timeout": "${test_timeout}"}}},
                    {"id": "merge_rerun", "type": "test_merge_results", "params": {"original": "${aggregate_results}", "rerun": "${rerun}"}}
                ]}},
                {"id": "generate_report", "type": "test_full_report", "params": {"results": "${merge_rerun}", "detail": "${report_detail}"}}},
                {"id": "cleanup_env", "type": "test_cleanup_env", "params": {"environment": "${test_environment}"}}
            ],
            requirements=["Testing framework", "Parallel execution capability"],
            comments=["Priority-based execution fails fast"]
        )
        
        # 29. API Tester
        api_tester = WorkflowTemplate(
            id="test_api_001",
            name="API Testing Suite",
            description="Comprehensive API testing with request building, response validation, and performance metrics",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.TESTING.value,
            tags=["api", "testing", "rest", "validation", "automation"],
            estimated_duration="15-45 minutes",
            difficulty_level=DifficultyLevel.ADVANCED.value,
            variables={
                "api_base_url": {"type": "string", "description": "API base URL", "required": True},
                "test_endpoints": {"type": "array", "description": "Endpoints to test", "required": True},
                "auth_token": {"type": "string", "description": "Authentication token", "required": False},
                "headers": {"type": "object", "description": "Default headers", "required": False},
                "validate_schema": {"type": "boolean", "description": "Validate response schema", "default": True},
                "check_performance": {"type": "boolean", "description": "Measure response times", "default": True},
                "max_response_time": {"type": "integer", "description": "Max acceptable ms", "default": 500},
                "iterations": {"type": "integer", "description": "Test iterations per endpoint", "default": 1}
            },
            steps=[
                {"id": "prepare_api", "type": "api_prepare", "params": {"base_url": "${api_base_url}", "auth": "${auth_token}", "headers": "${headers}"}}},
                {"id": "init_endpoint", "type": "variable_set", "params": {"var": "current_endpoint", "value": "${test_endpoints}[0]"}},
                {"id": "build_request", "type": "api_build_request", "params": {"endpoint": "${current_endpoint}"}}},
                {"id": "send_request", "type": "api_request", "params": {"request": "${build_request}"}}},
                {"id": "validate_status", "type": "api_validate_status", "params": {"response": "${send_request}", "expected": "${current_endpoint.expected_status}"}}},
                {"id": "validate_schema", "type": "conditional", "params": {"if": "${validate_schema}", "then": [
                    {"id": "schema_check", "type": "api_validate_schema", "params": {"response": "${send_request}", "schema": "${current_endpoint.schema}"}}
                ]}},
                {"id": "measure_performance", "type": "conditional", "params": {"if": "${check_performance}", "then": [
                    {"id": "timing", "type": "api_measure", "params": {"response": "${send_request}"}}},
                    {"id": "check_timing", "type": "conditional", "params": {"if": "${timing.response_time} > ${max_response_time}", "then": [
                        {"id": "mark_slow", "type": "test_mark_slow", "params": {"endpoint": "${current_endpoint}", "time": "${timing.response_time}"}}
                    ]}}
                ]}},
                {"id": "iterate_test", "type": "loop_count", "params": {"count": "${iterations}", "steps": ["send_request", "validate_status", "validate_schema", "measure_performance"]}},
                {"id": "next_endpoint", "type": "loop_next", "params": {"list": "${test_endpoints}", "var": "current_endpoint"}},
                {"id": "generate_report", "type": "api_test_report", "params": {"results": "all", "format": "html"}}
            ],
            requirements=["API client library", "JSON schema validator"],
            comments=["Includes performance benchmarking"]
        )
        
        # 30. Load Tester
        load_tester = WorkflowTemplate(
            id="test_load_001",
            name="Load Testing Framework",
            description="Simulate concurrent users, measure performance under load, identify bottlenecks",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.TESTING.value,
            tags=["load", "performance", "stress", "testing", "concurrent"],
            estimated_duration="30-120 minutes",
            difficulty_level=DifficultyLevel.EXPERT.value,
            variables={
                "target_url": {"type": "string", "description": "Target URL to test", "required": True},
                "test_type": {"type": "string", "description": "Type: load, stress, spike, soak", "default": "load"},
                "virtual_users": {"type": "integer", "description": "Concurrent virtual users", "default": 100},
                "ramp_up_seconds": {"type": "integer", "description": "Ramp-up time", "default": 60},
                "duration_seconds": {"type": "integer", "description": "Test duration", "default": 300},
                "requests_per_user": {"type": "integer", "description": "Requests per user", "default": 10},
                "think_time_ms": {"type": "integer", "description": "Delay between requests", "default": 1000},
                "thresholds": {"type": "object", "description": "Performance thresholds", "default": {"p95": 200, "error_rate": 1}},
                "generate_report": {"type": "boolean", "description": "Generate HTML report", "default": True}
            },
            steps=[
                {"id": "validate_target", "type": "http_check", "params": {"url": "${target_url}"}},
                {"id": "create_test_plan", "type": "load_create_plan", "params": {"target": "${target_url}", "vusers": "${virtual_users}", "ramp_up": "${ramp_up_seconds}", "duration": "${duration_seconds}"}}},
                {"id": "execute_load", "type": "load_execute", "params": {"plan": "${create_test_plan}", "requests": "${requests_per_user}", "think_time": "${think_time_ms}", "type": "${test_type}"}}},
                {"id": "collect_metrics", "type": "load_collect_metrics", "params": {"results": "${execute_load}"}}},
                {"id": "calculate_stats", "type": "load_calculate_stats", "params": {"metrics": "${collect_metrics}"}}},
                {"id": "check_thresholds", "type": "load_check_thresholds", "params": {"stats": "${calculate_stats}", "thresholds": "${thresholds}"}}},
                {"id": "identify_bottlenecks", "type": "load_analyze_bottlenecks", "params": {"metrics": "${collect_metrics}"}}},
                {"id": "generate_report", "type": "conditional", "params": {"if": "${generate_report}", "then": [
                    {"id": "create_report", "type": "load_report", "params": {"stats": "${calculate_stats}", "thresholds": "${thresholds}", "bottlenecks": "${identify_bottlenecks}"}}
                ]}},
                {"id": "alert_failures", "type": "conditional", "params": {"if": "${check_thresholds.failed}", "then": [
                    {"id": "notify_fail", "type": "notify", "params": {"title": "Load Test Failed", "body": "Thresholds exceeded: ${check_thresholds.details}"}}
                ]}}
            ],
            requirements=["Load testing tool (Locust, k6)", "Monitoring system"],
            comments=["Start low and gradually increase load"]
        )
        
        self.templates[smoke_test.id] = smoke_test
        self.templates[screenshot_diff.id] = screenshot_diff
        self.templates[regression_tester.id] = regression_tester
        self.templates[api_tester.id] = api_tester
        self.templates[load_tester.id] = load_tester

    # ========== DevOps Templates ==========
    
    def _load_devops_templates(self):
        """Load DevOps templates"""
        
        # 31. Health Checker
        health_checker = WorkflowTemplate(
            id="do_health_001",
            name="Service Health Checker",
            description="Check health of multiple services, endpoints, and dependencies with alerting",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.DEVOPS.value,
            tags=["health", "monitoring", "devops", "status", "checks"],
            estimated_duration="2-10 minutes",
            difficulty_level=DifficultyLevel.BEGINNER.value,
            variables={
                "services": {"type": "array", "description": "Services to check", "required": True},
                "check_types": {"type": "array", "description": "Check types: http, port, process, ping", "default": ["http"]},
                "timeout_seconds": {"type": "integer", "description": "Check timeout", "default": 10},
                "retry_count": {"type": "integer", "description": "Retries on failure", "default": 2},
                "alert_on_failure": {"type": "boolean", "description": "Send alerts on failures", "default": True},
                "alert_channels": {"type": "array", "description": "Alert methods", "default": ["log", "notify"]},
                "status_page_update": {"type": "boolean", "description": "Update status page", "default": False},
                "status_page_url": {"type": "string", "description": "Status page URL", "required": False}
            },
            steps=[
                {"id": "init_service", "type": "variable_set", "params": {"var": "current_service", "value": "${services}[0]"}},
                {"id": "http_check_op", "type": "conditional", "params": {"if": "'http' in ${check_types}", "then": [
                    {"id": "http_ping", "type": "http_health_check", "params": {"url": "${current_service.url}", "timeout": "${timeout_seconds}"}}},
                    {"id": "check_status", "type": "conditional", "params": {"if": "${http_ping.status} != 200", "then": [
                        {"id": "retry_http", "type": "http_retry", "params": {"url": "${current_service.url}", "count": "${retry_count}"}}
                    ]}}
                ]}},
                {"id": "port_check_op", "type": "conditional", "params": {"if": "'port' in ${check_types}", "then": [
                    {"id": "port_ping", "type": "network_port_check", "params": {"host": "${current_service.host}", "port": "${current_service.port}", "timeout": "${timeout_seconds}"}}
                ]}},
                {"id": "process_check_op", "type": "conditional", "params": {"if": "'process' in ${check_types}", "then": [
                    {"id": "process_ping", "type": "system_process_check", "params": {"process": "${current_service.process_name}"}}
                ]}},
                {"id": "aggregate_status", "type": "devops_aggregate_status", "params": {"checks": {"http": "${retry_http}", "port": "${port_ping}", "process": "${process_ping}"}}}},
                {"id": "alert_failure", "type": "conditional", "params": {"if": "${aggregate_status.is_down} and ${alert_on_failure}", "then": [
                    {"id": "send_alert", "type": "alert_batch", "params": {"channels": "${alert_channels}", "service": "${current_service}", "status": "${aggregate_status}"}}
                ]}},
                {"id": "update_status_page", "type": "conditional", "params": {"if": "${status_page_update}", "then": [
                    {"id": "status_update", "type": "http_post", "params": {"url": "${status_page_url}", "data": {"service": "${current_service.name}", "status": "${aggregate_status.status}"}}}
                ]}},
                {"id": "next_service", "type": "loop_next", "params": {"list": "${services}", "var": "current_service"}},
                {"id": "generate_summary", "type": "log", "params": {"message": "Health check: ${aggregate_status.up_count}/${services.length} services up", "level": "info"}}
            ],
            requirements=["Network access to services", "Alerting system"],
            comments=["Retry logic prevents false positives"]
        )
        
        # 32. Deployment Automation
        deployment_automation = WorkflowTemplate(
            id="do_deploy_001",
            name="Zero-Downtime Deployment",
            description="Automated deployment with blue-green strategy, health checks, and automatic rollback",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.DEVOPS.value,
            tags=["deployment", "devops", "automation", "rollback", "zero-downtime"],
            estimated_duration="15-45 minutes",
            difficulty_level=DifficultyLevel.EXPERT.value,
            variables={
                "application": {"type": "string", "description": "Application name", "required": True},
                "version": {"type": "string", "description": "Version to deploy", "required": True},
                "environment": {"type": "string", "description": "Target environment", "required": True},
                "deployment_strategy": {"type": "string", "description": "Strategy: blue-green, rolling, canary", "default": "blue-green"},
                "health_check_path": {"type": "string", "description": "Health check endpoint", "default": "/health"},
                "health_check_count": {"type": "integer", "description": "Successful checks before switch", "default": 3},
                "rollback_on_failure": {"type": "boolean", "description": "Auto-rollback on failure", "default": True},
                "backup_before": {"type": "boolean", "description": "Backup current version", "default": True},
                "notify_channels": {"type": "array", "description": "Notification channels", "default": ["log"]}
            },
            steps=[
                {"id": "validate_version", "type": "deploy_validate", "params": {"version": "${version}", "environment": "${environment}"}}},
                {"id": "backup_current", "type": "conditional", "params": {"if": "${backup_before}", "then": [
                    {"id": "create_backup", "type": "deploy_backup", "params": {"app": "${application}", "env": "${environment}"}}
                ]}},
                {"id": "prepare_deployment", "type": "deploy_prepare", "params": {"app": "${application}", "version": "${version}", "strategy": "${deployment_strategy}"}}},
                {"id": "deploy_blue", "type": "deploy_execute", "params": {"app": "${application}", "version": "${version}", "target": "blue", "strategy": "${deployment_strategy}"}}},
                {"id": "health_check_blue", "type": "deploy_health_check", "params": {"target": "blue", "path": "${health_check_path}", "count": "${health_check_count}"}}},
                {"id": "switch_traffic", "type": "conditional", "params": {"if": "${health_check_blue.passed}", "then": [
                    {"id": "switch", "type": "deploy_switch", "params": {"app": "${application}", "strategy": "${deployment_strategy}"}}
                ], "else": [
                    {"id": "rollback_trigger", "type": "conditional", "params": {"if": "${rollback_on_failure}", "then": [
                        {"id": "rollback_deploy", "type": "deploy_rollback", "params": {"app": "${application}", "backup": "${create_backup}"}},
                        {"id": "notify_fail", "type": "notify_batch", "params": {"channels": "${notify_channels}", "title": "Deployment Failed", "body": "Rolled back to previous version"}}
                    ]}}
                ]}},
                {"id": "verify_deployment", "type": "deploy_verify", "params": {"app": "${application}", "version": "${version}"}}},
                {"id": "cleanup_old", "type": "deploy_cleanup", "params": {"app": "${application}", "keep_versions": 3}}},
                {"id": "notify_success", "type": "notify_batch", "params": {"channels": "${notify_channels}", "title": "Deployment Successful", "body": "${application} v${version} deployed to ${environment}"}}
            ],
            requirements=["Container orchestration (K8s, Docker Swarm)", "Load balancer access"],
            comments=["Blue-green ensures zero downtime"]
        )
        
        # 33. Log Monitor
        log_monitor = WorkflowTemplate(
            id="do_log_001",
            name="Real-Time Log Monitor",
            description="Monitor logs in real-time, detect patterns, trigger alerts on errors or anomalies",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.DEVOPS.value,
            tags=["logs", "monitoring", "devops", "alerting", "real-time"],
            estimated_duration="Continuous",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "log_sources": {"type": "array", "description": "Log files or streams", "required": True},
                "patterns": {"type": "object", "description": "Patterns to detect", "required": True},
                "error_level": {"type": "string", "description": "Alert level: error, warning, info", "default": "error"},
                "alert_threshold": {"type": "integer", "description": "Alerts per minute threshold", "default": 10},
                "tail_lines": {"type": "integer", "description": "Lines to keep in buffer", "default": 1000},
                "alert_channels": {"type": "array", "description": "Alert methods", "default": ["log", "notify"]},
                "store_logs": {"type": "boolean", "description": "Store matched logs", "default": True},
                "store_path": {"type": "string", "description": "Storage path for logs", "default": "./monitored_logs"}
            },
            steps=[
                {"id": "init_source", "type": "variable_set", "params": {"var": "current_source", "value": "${log_sources}[0]"}},
                {"id": "open_stream", "type": "log_open_stream", "params": {"source": "${current_source}", "tail": "${tail_lines}"}}},
                {"id": "parse_log", "type": "log_parse", "params": {"stream": "${open_stream}", "format": "auto"}},
                {"id": "match_patterns", "type": "log_match", "params": {"logs": "${parse_log}", "patterns": "${patterns}"}}},
                {"id": "count_rate", "type": "log_count_rate", "params": {"matched": "${match_patterns}", "window_seconds": 60}}},
                {"id": "check_threshold", "type": "conditional", "params": {"if": "${count_rate.count} > ${alert_threshold}", "then": [
                    {"id": "rate_alert", "type": "alert_batch", "params": {"channels": "${alert_channels}", "title": "High Log Rate", "body": "${count_rate.count} matches/minute on ${current_source}"}}
                ]}},
                {"id": "filter_errors", "type": "log_filter_level", "params": {"logs": "${match_patterns}", "level": "${error_level}"}}},
                {"id": "alert_errors", "type": "conditional", "params": {"if": "${filter_errors.has_matches}", "then": [
                    {"id": "send_error_alert", "type": "alert_batch", "params": {"channels": "${alert_channels}", "logs": "${filter_errors}"}}
                ]}},
                {"id": "store_matches", "type": "conditional", "params": {"if": "${store_logs}", "then": [
                    {"id": "save_logs", "type": "file_append", "params": {"path": "${store_path}/${current_source.name}_${timestamp}.log", "data": "${match_patterns}"}}
                ]}},
                {"id": "next_source", "type": "loop_next", "params": {"list": "${log_sources}", "var": "current_source"}}
            ],
            requirements=["Log file access", "Pattern matching capability"],
            comments=["Rate limiting prevents alert floods"]
        )
        
        # 34. Database Backup
        database_backup = WorkflowTemplate(
            id="do_dbbackup_001",
            name="Automated Database Backup",
            description="Automated database backups with compression, encryption, and offsite replication",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.DEVOPS.value,
            tags=["database", "backup", "devops", "automation", "recovery"],
            estimated_duration="15-60 minutes",
            difficulty_level=DifficultyLevel.ADVANCED.value,
            variables={
                "database_connections": {"type": "array", "description": "Databases to backup", "required": True},
                "backup_destination": {"type": "string", "description": "Local backup path", "required": True},
                "remote_destination": {"type": "string", "description": "Remote storage URL", "required": False},
                "compression": {"type": "string", "description": "Compression: gzip, lz4, none", "default": "gzip"},
                "encrypt_backup": {"type": "boolean", "description": "Encrypt backups", "default": True},
                "encryption_key": {"type": "string", "description": "Encryption key", "sensitive": True, "required": False},
                "retention_days": {"type": "integer", "description": "Backup retention", "default": 30},
                "verify_backup": {"type": "boolean", "description": "Verify backup integrity", "default": True},
                "point_in_time": {"type": "boolean", "description": "Enable point-in-time recovery", "default": False}
            },
            steps=[
                {"id": "init_db", "type": "variable_set", "params": {"var": "current_db", "value": "${database_connections}[0]"}},
                {"id": "test_connection", "type": "db_ping", "params": {"connection": "${current_db}"}}},
                {"id": "create_dump", "type": "db_dump", "params": {"connection": "${current_db}", "pitr": "${point_in_time}"}}},
                {"id": "compress_dump", "type": "archive_compress", "params": {"input": "${create_dump.output}", "format": "${compression}"}}},
                {"id": "encrypt_dump", "type": "conditional", "params": {"if": "${encrypt_backup}", "then": [
                    {"id": "encrypt", "type": "encrypt_data", "params": {"data": "${compress_dump}", "key": "${encryption_key}", "algorithm": "AES256"}}
                ]}},
                {"id": "verify_dump", "type": "conditional", "params": {"if": "${verify_backup}", "then": [
                    {"id": "verify", "type": "db_verify_backup", "params": {"backup": "${encrypt_dump}", "connection": "${current_db}"}}
                ]}},
                {"id": "save_local", "type": "file_write", "params": {"path": "${backup_destination}/${current_db.name}_${timestamp}.${compression}", "data": "${encrypt_dump}"}}},
                {"id": "upload_remote", "type": "conditional", "params": {"if": "${remote_destination}", "then": [
                    {"id": "upload", "type": "file_upload", "params": {"file": "${save_local}", "url": "${remote_destination}/${current_db.name}/"}}
                ]}},
                {"id": "cleanup_old", "type": "file_cleanup_old", "params": {"path": "${backup_destination}", "pattern": "${current_db.name}*", "max_age_days": "${retention_days}"}}},
                {"id": "next_db", "type": "loop_next", "params": {"list": "${database_connections}", "var": "current_db"}},
                {"id": "report_backup", "type": "log", "params": {"message": "Database backup completed: ${database_connections.length} databases backed up", "level": "info"}}
            ],
            requirements=["Database client tools", "Storage access"],
            comments=["PITR enables point-in-time recovery"]
        )
        
        # 35. SSL Certificate Monitor
        ssl_monitor = WorkflowTemplate(
            id="do_ssl_001",
            name="SSL Certificate Monitor",
            description="Monitor SSL certificate expiration, automatically renew, and alert on issues",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.DEVOPS.value,
            tags=["ssl", "certificate", "monitoring", "devops", "security"],
            estimated_duration="5-15 minutes",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "domains": {"type": "array", "description": "Domains to monitor", "required": True},
                "warning_days": {"type": "integer", "description": "Warn days before expiry", "default": 30},
                "critical_days": {"type": "integer", "description": "Critical days before expiry", "default": 7},
                "check_ports": {"type": "array", "description": "Ports to check", "default": [443]},
                "auto_renew": {"type": "boolean", "description": "Auto-renew if supported", "default": False},
                "renew_command": {"type": "string", "description": "Renewal command", "required": False},
                "alert_channels": {"type": "array", "description": "Alert methods", "default": ["log", "notify"]},
                "store_results": {"type": "boolean", "description": "Store certificate info", "default": True}
            },
            steps=[
                {"id": "init_domain", "type": "variable_set", "params": {"var": "current_domain", "value": "${domains}[0]"}},
                {"id": "init_port", "type": "variable_set", "params": {"var": "current_port", "value": "${check_ports}[0]"}},
                {"id": "fetch_cert", "type": "ssl_fetch_cert", "params": {"domain": "${current_domain}", "port": "${current_port}"}}},
                {"id": "parse_cert", "type": "ssl_parse", "params": {"cert": "${fetch_cert}"}}},
                {"id": "check_expiry", "type": "ssl_check_expiry", "params": {"cert": "${parse_cert}", "warning_days": "${warning_days}", "critical_days": "${critical_days}"}}},
                {"id": "alert_critical_ssl", "type": "conditional", "params": {"if": "${check_expiry.is_critical}", "then": [
                    {"id": "critical_alert", "type": "alert_batch", "params": {"channels": "${alert_channels}", "title": "SSL Critical: ${current_domain}", "body": "Expires in ${check_expiry.days_remaining} days", "priority": "high"}}
                ]}},
                {"id": "alert_warning_ssl", "type": "conditional", "params": {"if": "${check_expiry.is_warning}", "then": [
                    {"id": "warning_alert", "type": "alert_batch", "params": {"channels": "${alert_channels}", "title": "SSL Warning: ${current_domain}", "body": "Expires in ${check_expiry.days_remaining} days"}}
                ]}},
                {"id": "auto_renew_ssl", "type": "conditional", "params": {"if": "${auto_renew} and ${check_expiry.is_critical} and ${renew_command}", "then": [
                    {"id": "execute_renew", "type": "shell_execute", "params": {"command": "${renew_command}", "domain": "${current_domain}"}}},
                    {"id": "verify_renew", "type": "ssl_fetch_cert", "params": {"domain": "${current_domain}", "port": "${current_port}"}}},
                    {"id": "check_renewal", "type": "conditional", "params": {"if": "${verify_renew.success}", "then": [
                        {"id": "renew_success", "type": "notify_batch", "params": {"channels": "${alert_channels}", "title": "SSL Renewed: ${current_domain}", "body": "Certificate renewed successfully"}}
                    ]}}
                ]}},
                {"id": "store_cert", "type": "conditional", "params": {"if": "${store_results}", "then": [
                    {"id": "save_cert", "type": "storage_set", "params": {"key": "ssl_cert_${current_domain}", "value": "${parse_cert}"}}
                ]}},
                {"id": "next_port", "type": "loop_next", "params": {"list": "${check_ports}", "var": "current_port"}},
                {"id": "next_domain", "type": "loop_next", "params": {"list": "${domains}", "var": "current_domain"}},
                {"id": "report_ssl", "type": "log", "params": {"message": "SSL check complete: ${domains.length} domains checked", "level": "info"}}
            ],
            requirements=["OpenSSL or certificate API access"],
            comments=["Proactive monitoring prevents outages"]
        )
        
        self.templates[health_checker.id] = health_checker
        self.templates[deployment_automation.id] = deployment_automation
        self.templates[log_monitor.id] = log_monitor
        self.templates[database_backup.id] = database_backup
        self.templates[ssl_monitor.id] = ssl_monitor

    # ========== Personal Automation Templates ==========
    
    def _load_personal_automation_templates(self):
        """Load personal automation templates"""
        
        # 36. Daily Standup
        daily_standup = WorkflowTemplate(
            id="pa_standup_001",
            name="Automated Daily Standup",
            description="Collect standup updates from team, aggregate, and post to channel with AI summaries",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.PERSONAL_AUTOMATION.value,
            tags=["standup", "daily", "team", "automation", "productivity"],
            estimated_duration="5-10 minutes",
            difficulty_level=DifficultyLevel.BEGINNER.value,
            variables={
                "team_members": {"type": "array", "description": "Team member emails", "required": True},
                "standup_time": {"type": "string", "description": "Collection time (HH:MM)", "default": "09:00"},
                "collection_method": {"type": "string", "description": "Method: email, slack, teams", "default": "slack"},
                "questions": {"type": "array", "description": "Standup questions", "default": ["Yesterday", "Today", "Blockers"]},
                "post_channel": {"type": "string", "description": "Channel to post summary", "required": True},
                "ai_summary": {"type": "boolean", "description": "Generate AI summary", "default": True},
                "include_metrics": {"type": "boolean", "description": "Include team metrics", "default": False},
                "timezone": {"type": "string", "description": "Team timezone", "default": "UTC"}
            },
            steps=[
                {"id": "schedule_collection", "type": "schedule_at", "params": {"time": "${standup_time}", "timezone": "${timezone}"}}},
                {"id": "send_requests", "type": "standup_collect", "params": {"members": "${team_members}", "method": "${collection_method}", "questions": "${questions}"}}},
                {"id": "wait_responses", "type": "wait_duration", "params": {"minutes": 30}}},
                {"id": "aggregate_responses", "type": "standup_aggregate", "params": {"responses": "received", "method": "${collection_method}"}}},
                {"id": "format_standup", "type": "standup_format", "params": {"responses": "${aggregate_responses}", "questions": "${questions}"}}},
                {"id": "ai_summary_gen", "type": "conditional", "params": {"if": "${ai_summary}", "then": [
                    {"id": "generate_summary", "type": "ai_summarize", "params": {"text": "${format_standup}", "max_points": 5, "style": "concise"}}
                ]}},
                {"id": "add_metrics", "type": "conditional", "params": {"if": "${include_metrics}", "then": [
                    {"id": "fetch_metrics", "type": "metrics_fetch", "params": {"team": "${team_members}", "period": "yesterday"}}}
                ]}},
                {"id": "post_summary", "type": "messaging_post", "params": {"channel": "${post_channel}", "message": "${ai_summary_gen}", "format": "rich"}},
                {"id": "store_standup", "type": "storage_append", "params": {"key": "standup_history", "value": {"date": "${today}", "standup": "${format_standup}"}, "ttl_days": 90}},
                {"id": "report_complete", "type": "log", "params": {"message": "Daily standup collected from ${team_members.length} team members", "level": "info"}}
            ],
            requirements=["Messaging platform integration"],
            comments=["Auto-posts at scheduled time"]
        )
        
        # 37. Expense Tracker
        expense_tracker = WorkflowTemplate(
            id="pa_expense_001",
            name="Personal Expense Tracker",
            description="Track expenses from multiple sources, categorize, budget, and generate spending reports",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.PERSONAL_AUTOMATION.value,
            tags=["expense", "budget", "finance", "tracking", "personal"],
            estimated_duration="5-15 minutes",
            difficulty_level=DifficultyLevel.BEGINNER.value,
            variables={
                "expense_sources": {"type": "array", "description": "Bank accounts, cards", "required": True},
                "categories": {"type": "object", "description": "Category rules", "default": {"food": ["restaurant", "grocery"], "transport": ["uber", "lyft", "gas"]}},
                "budget_monthly": {"type": "object", "description": "Monthly budget by category", "required": False},
                "auto_categorize": {"type": "boolean", "description": "Auto-categorize transactions", "default": True},
                "alert_threshold": {"type": "number", "description": "Alert at % of budget", "default": 80},
                "currency": {"type": "string", "description": "Currency code", "default": "USD"},
                "export_format": {"type": "string", "description": "Export: csv, json, pdf", "default": "csv"},
                "sync_frequency": {"type": "string", "description": "How often to sync: daily, weekly", "default": "daily"}
            },
            steps=[
                {"id": "init_source", "type": "variable_set", "params": {"var": "current_source", "value": "${expense_sources}[0]"}},
                {"id": "fetch_transactions", "type": "finance_fetch", "params": {"source": "${current_source}", "period": "since_last_sync"}}},
                {"id": "dedupe_transactions", "type": "list_deduplicate", "params": {"list": "${fetch_transactions}", "by": "transaction_id"}},
                {"id": "categorize_expenses", "type": "conditional", "params": {"if": "${auto_categorize}", "then": [
                    {"id": "ai_categorize", "type": "ai_categorize", "params": {"transactions": "${dedupe_transactions}", "rules": "${categories}"}}
                ]}},
                {"id": "detect_merchant", "type": "finance_detect_merchant", "params": {"transactions": "${categorize_expenses}"}},
                {"id": "store_transactions", "type": "storage_append", "params": {"key": "expenses_${current_source}", "value": "${detect_merchant}"}}},
                {"id": "check_budget", "type": "finance_check_budget", "params": {"expenses": "${detect_merchant}", "budget": "${budget_monthly}"}}},
                {"id": "alert_budget", "type": "conditional", "params": {"if": "${check_budget.over_threshold}", "then": [
                    {"id": "send_alert", "type": "notify", "params": {"title": "Budget Alert: ${check_budget.category}", "body": "${check_budget.spent} of ${check_budget.budget} (${check_budget.percent}%)"}}
                ]}},
                {"id": "next_source", "type": "loop_next", "params": {"list": "${expense_sources}", "var": "current_source"}},
                {"id": "generate_report", "type": "finance_report", "params": {"period": "monthly", "format": "${export_format}"}}},
                {"id": "log_expenses", "type": "log", "params": {"message": "Tracked ${detect_merchant.count} expenses totaling ${detect_merchant.total}", "level": "info"}}
            ],
            requirements=["Bank/financial API access"],
            comments=["Supports multiple accounts and currencies"]
        )
        
        # 38. Time Logger
        time_logger = WorkflowTemplate(
            id="pa_time_001",
            name="Productivity Time Logger",
            description="Track time spent on projects and tasks, generate reports, and analyze productivity",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.PERSONAL_AUTOMATION.value,
            tags=["time", "tracking", "productivity", "log", " Pomodoro"],
            estimated_duration="Ongoing",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "projects": {"type": "array", "description": "Project list to track", "required": True},
                "tracking_method": {"type": "string", "description": "Method: manual, auto_detect, calendar", "default": "manual"},
                "default_task": {"type": "string", "description": "Default task for unspecific time", "default": "General"},
                "pomodoro_length": {"type": "integer", "description": "Pomodoro length in minutes", "default": 25},
                "break_length": {"type": "integer", "description": "Break length in minutes", "default": 5},
                "auto_break": {"type": "boolean", "description": "Auto-start breaks", "default": False},
                "weekly_report": {"type": "boolean", "description": "Generate weekly report", "default": True},
                "sync_calendar": {"type": "boolean", "description": "Sync with calendar", "default": False}
            },
            steps=[
                {"id": "start_tracker", "type": "time_start", "params": {"project": "${projects}[0]", "method": "${tracking_method}"}}},
                {"id": "track_loop", "type": "loop_with_timer", "params": {"duration_minutes": "${pomodoro_length}"}}},
                {"id": "log_period", "type": "time_log", "params": {"project": "${projects}[0]", "task": "${default_task}", "duration": "${pomodoro_length}"}}},
                {"id": "prompt_break", "type": "user_input", "params": {"prompt": "Take a ${break_length} minute break?", "type": "confirm"}},
                {"id": "start_break", "type": "conditional", "params": {"if": "${prompt_break} and ${auto_break}", "then": [
                    {"id": "break_timer", "type": "wait_duration", "params": {"minutes": "${break_length}"}},
                    {"id": "notify_break", "type": "notify", "params": {"title": "Break Over", "body": "Ready to continue?"}}
                ]}},
                {"id": "switch_project", "type": "user_input", "params": {"prompt": "Switch project?", "type": "select", "options": "${projects}"}}},
                {"id": "continue_tracking", "type": "conditional", "params": {"if": "${switch_project}", "then": [
                    {"id": "switch", "type": "time_switch", "params": {"project": "${switch_project}"}}
                ]}},
                {"id": "stop_tracking", "type": "time_stop", "params": {}},
                {"id": "generate_report", "type": "conditional", "params": {"if": "${weekly_report}", "then": [
                    {"id": "create_report", "type": "time_report", "params": {"period": "week", "projects": "${projects}"}}},
                    {"id": "analyze_productivity", "type": "time_analyze", "params": {"data": "${create_report}"}}
                ]}},
                {"id": "sync_cal", "type": "conditional", "params": {"if": "${sync_calendar}", "then": [
                    {"id": "calendar_sync", "type": "calendar_create_events", "params": {"time_entries": "${create_report}"}}
                ]}}
            ],
            requirements=["Time tracking integration", "Calendar API (optional)"],
            comments=["Pomodoro technique built-in"]
        )
        
        # 39. Personal Dashboard
        personal_dashboard = WorkflowTemplate(
            id="pa_dashboard_001",
            name="Personal Life Dashboard",
            description="Aggregate personal metrics: calendar, tasks, habits, health, and finance into one view",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.PERSONAL_AUTOMATION.value,
            tags=["dashboard", "personal", "metrics", "life", "tracking"],
            estimated_duration="10-20 minutes",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "data_sources": {"type": "array", "description": "Data sources to aggregate", "required": True},
                "refresh_interval": {"type": "integer", "description": "Minutes between refreshes", "default": 60},
                "metrics_to_show": {"type": "array", "description": "Metrics to display", "default": ["calendar", "tasks", "habits", "health", "finance"]},
                "goal_tracking": {"type": "boolean", "description": "Enable goal tracking", "default": True},
                "habit_streaks": {"type": "boolean", "description": "Track habit streaks", "default": True},
                "export_dashboard": {"type": "boolean", "description": "Export dashboard data", "default": False},
                "share_with": {"type": "array", "description": "People to share with", "required": False}
            },
            steps=[
                {"id": "fetch_calendar", "type": "conditional", "params": {"if": "'calendar' in ${metrics_to_show}", "then": [
                    {"id": "get_calendar", "type": "calendar_fetch_today", "params": {}}
                ]}},
                {"id": "fetch_tasks", "type": "conditional", "params": {"if": "'tasks' in ${metrics_to_show}", "then": [
                    {"id": "get_tasks", "type": "tasks_fetch", "params": {"filter": "today"}}}
                ]}},
                {"id": "fetch_habits", "type": "conditional", "params": {"if": "'habits' in ${metrics_to_show}", "then": [
                    {"id": "get_habits", "type": "habits_fetch", "params": {}}}
                ]}},
                {"id": "fetch_health", "type": "conditional", "params": {"if": "'health' in ${metrics_to_show}", "then": [
                    {"id": "get_health", "type": "health_fetch", "params": {"metrics": ["steps", "sleep", "water"]}}}
                ]}},
                {"id": "fetch_finance", "type": "conditional", "params": {"if": "'finance' in ${metrics_to_show}", "then": [
                    {"id": "get_finance", "type": "finance_fetch_today", "params": {}}}
                ]}},
                {"id": "calculate_goals", "type": "conditional", "params": {"if": "${goal_tracking}", "then": [
                    {"id": "track_goals", "type": "goals_check", "params": {"sources": ["${get_tasks}", "${get_habits}", "${get_health}"]}}
                ]}},
                {"id": "calculate_streaks", "type": "conditional", "params": {"if": "${habit_streaks}", "then": [
                    {"id": "track_streaks", "type": "habits_calculate_streaks", "params": {"habits": "${get_habits}"}}
                ]}},
                {"id": "aggregate_dashboard", "type": "dashboard_build", "params": {"calendar": "${get_calendar}", "tasks": "${get_tasks}", "habits": "${get_habits}", "health": "${get_health}", "finance": "${get_finance}", "goals": "${track_goals}", "streaks": "${track_streaks}"}}},
                {"id": "render_dashboard", "type": "dashboard_render", "params": {"data": "${aggregate_dashboard}", "format": "html"}},
                {"id": "export_dash", "type": "conditional", "params": {"if": "${export_dashboard}", "then": [
                    {"id": "export_data", "type": "file_write", "params": {"path": "./dashboard_${date}.json", "data": "${aggregate_dashboard}"}}
                ]}},
                {"id": "share_dash", "type": "conditional", "params": {"if": "${share_with}", "then": [
                    {"id": "share_data", "type": "messaging_send", "params": {"to": "${share_with}", "message": "My Dashboard: ${aggregate_dashboard.summary}"}}
                ]}}
            ],
            requirements=["Calendar, task, health API integrations"],
            comments=["One view of your entire life"]
        )
        
        # 40. Reading List Manager
        reading_list = WorkflowTemplate(
            id="pa_reading_001",
            name="Smart Reading List Manager",
            description="Collect articles from multiple sources, summarize, and create reading schedule",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.PERSONAL_AUTOMATION.value,
            tags=["reading", "articles", "summarize", "personal", "knowledge"],
            estimated_duration="10-30 minutes",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "sources": {"type": "array", "description": "RSS feeds, bookmarks, pockets", "required": True},
                "keywords_filter": {"type": "array", "description": "Filter by keywords", "required": False},
                "max_articles": {"type": "integer", "description": "Max articles to process", "default": 20},
                "summarize_articles": {"type": "boolean", "description": "AI summarize articles", "default": True},
                "reading_time_max": {"type": "integer", "description": "Max reading time in minutes", "default": 30},
                "schedule_reading": {"type": "boolean", "description": "Create reading schedule", "default": True},
                "export_format": {"type": "string", "description": "Export: epub, pdf, html", "default": "html"},
                "save_location": {"type": "string", "description": "Save location", "default": "./reading_list"}
            },
            steps=[
                {"id": "init_source", "type": "variable_set", "params": {"var": "current_source", "value": "${sources}[0]"}},
                {"id": "fetch_articles", "type": "rss_fetch", "params": {"source": "${current_source}", "limit": "${max_articles}"}}},
                {"id": "filter_keywords", "type": "conditional", "params": {"if": "${keywords_filter}", "then": [
                    {"id": "filter", "type": "list_filter_keyword", "params": {"list": "${fetch_articles}", "keywords": "${keywords_filter}"}}
                ]}},
                {"id": "fetch_content", "type": "article_fetch_content", "params": {"articles": "${filter_keywords}"}}},
                {"id": "summarize_articles", "type": "conditional", "params": {"if": "${summarize_articles}", "then": [
                    {"id": "ai_summarize", "type": "ai_summarize_batch", "params": {"articles": "${fetch_content}", "max_length": 200}}
                ]}},
                {"id": "calculate_reading_time", "type": "article_estimate_time", "params": {"articles": "${summarize_articles}"}}},
                {"id": "filter_by_time", "type": "list_filter", "params": {"list": "${calculate_reading_time}", "by": "reading_time", "max": "${reading_time_max}"}}},
                {"id": "create_schedule", "type": "conditional", "params": {"if": "${schedule_reading}", "then": [
                    {"id": "schedule", "type": "reading_schedule", "params": {"articles": "${filter_by_time}", "daily_minutes": "${reading_time_max}"}}}
                ]}},
                {"id": "export_articles", "type": "article_export", "params": {"articles": "${filter_by_time}", "format": "${export_format}", "path": "${save_location}"}}},
                {"id": "add_to_reader", "type": "pocket_add", "params": {"articles": "${filter_by_time}"}},
                {"id": "next_source", "type": "loop_next", "params": {"list": "${sources}", "var": "current_source"}},
                {"id": "report_reading", "type": "log", "params": {"message": "Processed ${filter_by_time.count} articles for reading", "level": "info"}}
            ],
            requirements=["RSS/API access to sources", "AI summarization (optional)"],
            comments=["Prioritizes by relevance and reading time"]
        )
        
        # Bonus Templates (2 more for 40+ total)
        
        # 41. Password Manager
        password_manager = WorkflowTemplate(
            id="pa_password_001",
            name="Password Health Manager",
            description="Audit passwords for strength, detect breaches, auto-generate secure passwords",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.PERSONAL_AUTOMATION.value,
            tags=["password", "security", "breach", "audit", "generator"],
            estimated_duration="5-15 minutes",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "password_store": {"type": "string", "description": "Password database path", "required": True},
                "check_breaches": {"type": "boolean", "description": "Check HaveIBeenPwned", "default": True},
                "min_password_score": {"type": "integer", "description": "Minimum acceptable score (0-100)", "default": 60},
                "auto_generate_strong": {"type": "boolean", "description": "Auto-generate for weak passwords", "default": False},
                "password_length": {"type": "integer", "description": "Generated password length", "default": 16},
                "include_symbols": {"type": "boolean", "description": "Include symbols in generated", "default": True},
                "alert_on_breach": {"type": "boolean", "description": "Alert if password breached", "default": True},
                "export_audit": {"type": "boolean", "description": "Export audit report", "default": True}
            },
            steps=[
                {"id": "load_passwords", "type": "password_load", "params": {"store": "${password_store}"}}},
                {"id": "audit_strength", "type": "password_audit", "params": {"passwords": "${load_passwords}"}}},
                {"id": "filter_weak", "type": "list_filter", "params": {"list": "${audit_strength}", "by": "score", "min": "${min_password_score}"}}},
                {"id": "check_breaches_op", "type": "conditional", "params": {"if": "${check_breaches}", "then": [
                    {"id": "check_hibp", "type": "password_breach_check", "params": {"passwords": "${load_passwords}"}}},
                    {"id": "filter_breached", "type": "list_filter", "params": {"list": "${check_hibp}", "by": "breached", "value": True}}},
                    {"id": "alert_breach", "type": "conditional", "params": {"if": "${alert_on_breach} and ${filter_breached.length} > 0", "then": [
                        {"id": "send_alert", "type": "notify", "params": {"title": "Password Breach Alert", "body": "${filter_breached.length} passwords found in breaches"}}
                    ]}}
                ]}},
                {"id": "generate_strong", "type": "conditional", "params": {"if": "${auto_generate_strong} and ${filter_weak.length} > 0", "then": [
                    {"id": "generate_passwords", "type": "password_generate_batch", "params": {"count": "${filter_weak.length}", "length": "${password_length}", "symbols": "${include_symbols}"}}},
                    {"id": "update_weak", "type": "password_update_batch", "params": {"updates": "${filter_weak}", "new_passwords": "${generate_passwords}"}}
                ]}},
                {"id": "export_report", "type": "conditional", "params": {"if": "${export_audit}", "then": [
                    {"id": "create_report", "type": "password_report", "params": {"audited": "${audit_strength}", "breached": "${filter_breached}", "weak": "${filter_weak}"}}},
                    {"id": "save_report", "type": "file_write", "params": {"path": "./password_audit_${timestamp}.json", "data": "${create_report}"}}
                ]}},
                {"id": "report_pass", "type": "log", "params": {"message": "Password audit: ${filter_breached.length} breached, ${filter_weak.length} weak", "level": "warning"}}
            ],
            requirements=["Password database access", "HaveIBeenPwned API"],
            comments=["Never stores actual passwords in logs"]
        )
        
        # 42. Email Cleanup
        email_cleanup = WorkflowTemplate(
            id="pa_email_cleanup_001",
            name="Email Archive & Cleanup",
            description="Clean up email inbox, archive old messages, unsubscribe from lists, find large attachments",
            version="1.0.0",
            author="Template Library",
            category=TemplateCategory.PERSONAL_AUTOMATION.value,
            tags=["email", "cleanup", "archive", "unsubscribe", "inbox"],
            estimated_duration="15-45 minutes",
            difficulty_level=DifficultyLevel.INTERMEDIATE.value,
            variables={
                "email_account": {"type": "string", "description": "Email account", "required": True},
                "clean_before_date": {"type": "date", "description": "Archive emails before this date", "required": True},
                "find_attachments": {"type": "boolean", "description": "Find large attachments", "default": True},
                "attachment_size_mb": {"type": "integer", "description": "Large attachment threshold MB", "default": 10},
                "unsubscribe_old": {"type": "boolean", "description": "Find and unsubscribe options", "default": True},
                "labels_to_clean": {"type": "array", "description": "Labels/folders to clean", "default": ["Promotions", "Social"]},
                "archive_instead_delete": {"type": "boolean", "description": "Archive instead of delete", "default": True},
                "dry_run": {"type": "boolean", "description": "Preview without changes", "default": True},
                "report_email": {"type": "string", "description": "Email to send report", "required": False}
            },
            steps=[
                {"id": "connect_email", "type": "email_connect", "params": {"account": "${email_account}"}}},
                {"id": "search_old_emails", "type": "email_search", "params": {"account": "${email_account}", "before": "${clean_before_date}", "labels": "${labels_to_clean}"}}},
                {"id": "find_attachments_op", "type": "conditional", "params": {"if": "${find_attachments}", "then": [
                    {"id": "scan_attachments", "type": "email_attachment_scan", "params": {"emails": "${search_old_emails}", "min_size_mb": "${attachment_size_mb}"}}},
                    {"id": "report_attachments", "type": "log", "params": {"message": "Found ${scan_attachments.count} large attachments totaling ${scan_attachments.size_mb}MB", "level": "info"}}
                ]}},
                {"id": "find_subscriptions", "type": "conditional", "params": {"if": "${unsubscribe_old}", "then": [
                    {"id": "detect_subscriptions", "type": "email_detect_subscriptions", "params": {"emails": "${search_old_emails}"}}},
                    {"id": "offer_unsubscribe", "type": "user_input", "params": {"prompt": "Unsubscribe from ${detect_subscriptions.length} senders?", "type": "confirm"}},
                    {"id": "execute_unsubscribe", "type": "conditional", "params": {"if": "${offer_unsubscribe} and not ${dry_run}", "then": [
                        {"id": "unsub_batch", "type": "email_unsubscribe_batch", "params": {"subscriptions": "${detect_subscriptions}"}}
                    ]}}
                ]}},
                {"id": "archive_emails", "type": "conditional", "params": {"if": "${archive_instead_delete}", "then": [
                    {"id": "archive_batch", "type": "email_archive_batch", "params": {"emails": "${search_old_emails}", "label": "Archive_${clean_before_date}"}}
                ], "else": [
                    {"id": "delete_emails", "type": "email_delete_batch", "params": {"emails": "${search_old_emails}"}}
                ]}},
                {"id": "generate_report", "type": "email_cleanup_report", "params": {"archived": "${archive_batch.count}", "deleted": "${delete_emails.count}", "attachments": "${scan_attachments}"}}},
                {"id": "send_report", "type": "conditional", "params": {"if": "${report_email}", "then": [
                    {"id": "email_report", "type": "email_send", "params": {"to": "${report_email}", "subject": "Email Cleanup Report", "body": "${generate_report}"}}
                ]}},
                {"id": "log_cleanup", "type": "log", "params": {"message": "Email cleanup: ${archive_batch.count} emails archived", "level": "info"}}
            ],
            requirements=["Email IMAP access"],
            comments=["Archive-first approach prevents data loss"]
        )
        
        self.templates[daily_standup.id] = daily_standup
        self.templates[expense_tracker.id] = expense_tracker
        self.templates[time_logger.id] = time_logger
        self.templates[personal_dashboard.id] = personal_dashboard
        self.templates[reading_list.id] = reading_list
        self.templates[password_manager.id] = password_manager
        self.templates[email_cleanup.id] = email_cleanup


# Singleton instance for easy access
_library_instance = None

def get_template_library() -> TemplateLibrary:
    """Get the singleton template library instance"""
    global _library_instance
    if _library_instance is None:
        _library_instance = TemplateLibrary()
    return _library_instance


if __name__ == "__main__":
    # Demo: print all templates
    library = get_template_library()
    print(f"Template Library loaded with {len(library.templates)} templates")
    print("\nTemplates by Category:")
    
    for category in TemplateCategory:
        templates = library.get_templates_by_category(category)
        print(f"\n{category.value}: {len(templates)} templates")
        for t in templates:
            print(f"  - {t.id}: {t.name}")
