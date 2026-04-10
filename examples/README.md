# RabAI AutoClick Example Workflows

This directory contains comprehensive example workflows demonstrating the capabilities of RabAI AutoClick.

## Workflow Index

| # | Workflow File | Description | Steps |
|---|---------------|-------------|-------|
| 1 | `workflows/web_scraper.yaml` | Navigate to a website, extract data using OCR, and save results | 15 |
| 2 | `workflows/auto_backup.yaml` | Backup critical files to a designated backup location with verification | 17 |
| 3 | `workflows/social_poster.yaml` | Automate posting the same message across Twitter, LinkedIn, and Facebook | 18 |
| 4 | `workflows/data_entry.yaml` | Fill out a web form with data from structured variables | 26 |
| 5 | `workflows/monitoring.yaml` | Monitor system resources (CPU, memory, disk) and capture evidence | 24 |
| 6 | `workflows/batch_rename.yaml` | Rename a batch of files with sequential numbering and prefixes | 23 |
| 7 | `workflows/auto_reply.yaml` | Check inbox, identify messages, and send automated replies | 24 |
| 8 | `workflows/screenshot_archive.yaml` | Capture periodic screenshots and save with timestamps | 27 |

## Workflow Descriptions

### 1. Web Scraper (`workflows/web_scraper.yaml`)
Multi-step web scraping workflow that:
- Opens a browser and navigates to a target URL
- Scrolls to reveal content
- Captures screenshots of web pages
- Uses OCR to extract text from images
- Copies and pastes content into a text editor
- Saves the scraped data to a file

### 2. Auto Backup (`workflows/auto_backup.yaml`)
File backup automation workflow that:
- Opens Finder and navigates to source folder
- Selects all files in the folder
- Copies files to clipboard
- Creates a timestamped backup folder
- Pastes files into the backup location
- Verifies the backup with a screenshot

### 3. Social Poster (`workflows/social_poster.yaml`)
Multi-platform social media posting workflow that:
- Opens browser and navigates to Twitter
- Composes and posts a tweet
- Navigates to LinkedIn
- Composes and posts a LinkedIn update
- Captures confirmation screenshots

### 4. Data Entry (`workflows/data_entry.yaml`)
Form filling automation workflow that:
- Navigates to a web form
- Fills in text fields (first name, last name, email, phone)
- Uses Tab key to navigate between fields
- Fills address information (street, city, state, ZIP)
- Submits the completed form
- Verifies submission with a screenshot

### 5. Monitoring (`workflows/monitoring.yaml`)
System monitoring with alerts workflow that:
- Opens Activity Monitor via Spotlight
- Captures screenshots of CPU usage
- Captures screenshots of Memory usage
- Opens Terminal to run disk usage commands
- Captures disk space information
- Runs top processes command
- Documents all system metrics

### 6. Batch Rename (`workflows/batch_rename.yaml`)
Batch file rename workflow that:
- Opens Finder and navigates to target folder
- Selects all files
- Opens macOS Finder's built-in rename function
- Configures sequential naming with custom prefix
- Sets number padding (e.g., 001, 002)
- Applies the rename operation
- Verifies the results

### 7. Auto Reply (`workflows/auto_reply.yaml`)
Automated email/message reply workflow that:
- Navigates to Gmail inbox
- Captures initial inbox state
- Opens first unread email
- Captures email content
- Clicks reply and enters templated response
- Sends the reply
- Repeats for second unread email
- Documents the completion

### 8. Screenshot Archive (`workflows/screenshot_archive.yaml`)
Periodic screenshot capture workflow that:
- Creates a timestamped project folder
- Minimizes Finder to capture active screen
- Captures 5 screenshots at configurable intervals
- Organizes screenshots in a dedicated archive folder
- Opens Finder to display the captured archive
- Documents the complete archive

## Usage

To run any workflow:

```bash
python -m rabai_autoclick --workflow examples/workflows/<workflow_name>.yaml
```

Or use the GUI to load and execute workflows.

## Variables

Most workflows use variables that can be customized:
- `target_url` - URL to navigate to
- `source_folder` / `backup_folder` - File paths
- `message` / `template_response` - Text content
- `prefix` / `start_number` - Rename patterns
- `interval_seconds` - Time between screenshots

Modify these variables before running to customize the workflow behavior.
