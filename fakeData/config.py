"""
Configuration file for exam schedule generator.
Fill in your own API keys and database connection details.
"""

# DeepSeek API configuration
DEEPSEEK_API = {
    'url': 'https://api.deepseek.com/v1/chat/completions',
    'key': '',  # Add your DeepSeek API key here
    'model': 'deepseek-chat',
    'temperature': 0.1,
    'max_tokens': 4000
}

# MySQL database configuration
DATABASE = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'port': 3306,
    'database': 'exam_schedule'
}

# Scheduling constraints
SCHEDULING = {
    'time_slots': ["8:30-10:30", "11:30-1:30", "2:30-4:30"],
    'min_days_between_exams': 1,
    'no_same_day_exams': True,
    'scheduling_window_days': 21,  # Try to schedule all exams within this many days
    'start_date_offset': 30,  # Start scheduling this many days from now
    'skip_weekends': True
}

# Output settings
OUTPUT = {
    'default_csv_filename': 'exam_schedule_output.csv',
    'save_to_database': False
} 