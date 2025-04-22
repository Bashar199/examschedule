import csv
import mysql.connector

# MySQL connection configuration (XAMPP defaults)
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'port': 3306
}

DATABASE_NAME = 'exam_schedule'

def create_database_and_tables(connection):
    """Create the database and tables for each program and academic level"""
    cursor = connection.cursor()
    
    # Create database if it doesn't exist
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
    cursor.execute(f"USE {DATABASE_NAME}")
    
    # Create CE courses tables for each level
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ce_diploma_courses (
        code VARCHAR(10) PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ce_advanced_courses (
        code VARCHAR(10) PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ce_bachelor_courses (
        code VARCHAR(10) PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    )
    """)
    
    # Create ECE courses tables for each level
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ece_diploma_courses (
        code VARCHAR(10) PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ece_advanced_courses (
        code VARCHAR(10) PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ece_bachelor_courses (
        code VARCHAR(10) PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    )
    """)
    
    # Create EEE courses tables for each level
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS eee_diploma_courses (
        code VARCHAR(10) PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS eee_advanced_courses (
        code VARCHAR(10) PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS eee_bachelor_courses (
        code VARCHAR(10) PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    )
    """)
    
    # Create exams table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS exams (
        course_code VARCHAR(10),
        time VARCHAR(20),
        course_title VARCHAR(255),
        section VARCHAR(10),
        num_students INT,
        teacher_name VARCHAR(255),
        PRIMARY KEY (course_code, section)
    )
    """)
    
    connection.commit()
    cursor.close()

def load_course_data(connection, csv_file, program_prefix):
    """Load data from a course CSV file into the corresponding tables by academic level"""
    cursor = connection.cursor()
    diploma_count = 0
    advanced_count = 0
    bachelor_count = 0
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            current_section = None
            
            for row in csv_reader:
                # Skip empty rows
                if not row:
                    continue
                
                # Detect section headers
                if row[0].startswith('#'):
                    if 'DIPLOMA' in row[0] and 'SECOND' in row[0]:
                        current_section = 'diploma'
                    elif 'ADVANCE' in row[0]:
                        current_section = 'advanced'
                    elif 'BACHELOR' in row[0]:
                        current_section = 'bachelor'
                    continue
                
                # Skip if not in a recognized section or header row
                if not current_section or row[0] == 'Code' or row[0].startswith('Code,'):
                    continue
                
                # Process data row
                if len(row) >= 2:
                    code = row[0].strip()
                    name = row[1].strip()
                    
                    # If the row only has one element but contains a comma, split it
                    if len(row) == 1 and ',' in row[0]:
                        parts = row[0].split(',', 1)
                        if len(parts) == 2:
                            code = parts[0].strip()
                            name = parts[1].strip()
                    
                    if code and name:
                        table_name = f"{program_prefix}_{current_section}_courses"
                        try:
                            cursor.execute(f"""
                            INSERT IGNORE INTO {table_name} (code, name) 
                            VALUES (%s, %s)
                            """, (code, name))
                            
                            if current_section == 'diploma':
                                diploma_count += cursor.rowcount
                            elif current_section == 'advanced':
                                advanced_count += cursor.rowcount
                            elif current_section == 'bachelor':
                                bachelor_count += cursor.rowcount
                                
                        except Exception as e:
                            print(f"Error inserting course {code} into {table_name}: {e}")
    except Exception as e:
        print(f"Error processing {csv_file}: {e}")
    
    connection.commit()
    cursor.close()
    print(f"Program {program_prefix.upper()}: Inserted {diploma_count} diploma courses, {advanced_count} advanced courses, and {bachelor_count} bachelor courses")

def load_exam_data(connection):
    """Load exam data from exam_schedule.csv"""
    cursor = connection.cursor()
    exam_count = 0
    
    try:
        with open('exam_schedule.csv', 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)  # Skip header
            
            for row in csv_reader:
                if len(row) >= 6:
                    course_code = row[0].strip()
                    time = row[1].strip()
                    course_title = row[2].strip()
                    section = row[3].strip() if row[3].strip() else "1"  # Default to 1 if empty
                    
                    # Handle empty num_students
                    try:
                        num_students = int(row[4].strip()) if row[4].strip() else 0
                    except ValueError:
                        num_students = 0
                    
                    teacher_name = row[5].strip()
                    
                    try:
                        cursor.execute("""
                        INSERT INTO exams 
                        (course_code, time, course_title, section, num_students, teacher_name)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        time = VALUES(time),
                        course_title = VALUES(course_title),
                        num_students = VALUES(num_students),
                        teacher_name = VALUES(teacher_name)
                        """, (course_code, time, course_title, section, num_students, teacher_name))
                        exam_count += cursor.rowcount
                    except Exception as e:
                        print(f"Error inserting exam for {course_code}: {e}")
    except Exception as e:
        print(f"Error processing exam_schedule.csv: {e}")
    
    connection.commit()
    cursor.close()
    print(f"Inserted {exam_count} exams")

def main():
    try:
        # Connect to MySQL
        connection = mysql.connector.connect(**DB_CONFIG)
        print("Connected to MySQL server")
        
        # Create database and tables
        create_database_and_tables(connection)
        print(f"Database '{DATABASE_NAME}' and tables created successfully")
        
        # Switch to using the created database
        connection.database = DATABASE_NAME
        
        # Load course data
        load_course_data(connection, 'CE.csv', 'ce')
        load_course_data(connection, 'ECE.csv', 'ece')
        load_course_data(connection, 'EEE.csv', 'eee')
        
        # Load exam data
        load_exam_data(connection)
        
        print("Data import completed successfully")
        connection.close()
        
    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
