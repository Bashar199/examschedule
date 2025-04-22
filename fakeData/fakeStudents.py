import mysql.connector
import random
from faker import Faker
import datetime

# Initialize Faker
fake = Faker()

# MySQL connection configuration (XAMPP defaults)
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'port': 3306,
    'database': 'exam_schedule'
}

# Constants
TOTAL_STUDENTS = 600
STUDENTS_PER_DEPT = 200
DIPLOMA_PERCENT = 0.6
ADVANCED_PERCENT = 0.2
BACHELOR_PERCENT = 0.2
MIXED_COURSES_PERCENT = 0.15
MAX_MIXED_COURSES = 4

# Student ID format: YYYY + Department Code + Sequential Number
# Example: 2023CE001, 2023ECE002, etc.
CURRENT_YEAR = datetime.datetime.now().year
DEPARTMENTS = ["CE", "ECE", "EEE"]
ACADEMIC_LEVELS = ["diploma", "advanced", "bachelor"]

def create_database_tables(connection):
    """Create student and enrollment tables"""
    cursor = connection.cursor()
    
    # Create students table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS students (
        id VARCHAR(20) PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        department VARCHAR(5) NOT NULL,
        academic_level VARCHAR(20) NOT NULL,
        email VARCHAR(100),
        phone VARCHAR(20),
        address VARCHAR(255),
        mixed_courses BOOLEAN DEFAULT FALSE
    )
    """)
    
    # Create enrollments table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS enrollments (
        id INT AUTO_INCREMENT PRIMARY KEY,
        student_id VARCHAR(20) NOT NULL,
        course_code VARCHAR(10) NOT NULL,
        FOREIGN KEY (student_id) REFERENCES students(id)
    )
    """)
    
    connection.commit()
    cursor.close()

def get_courses_by_department_level(connection, department, level):
    """Get all courses for a specific department and level"""
    cursor = connection.cursor()
    
    try:
        cursor.execute(f"""
        SELECT code FROM {department.lower()}_{level}_courses
        """)
        courses = [row[0] for row in cursor.fetchall()]
        return courses
    except Exception as e:
        print(f"Error getting courses for {department} {level}: {e}")
        return []
    finally:
        cursor.close()

def generate_students(connection):
    """Generate and insert fake student data"""
    cursor = connection.cursor()
    
    # Calculate students per academic level for each department
    diploma_count = int(STUDENTS_PER_DEPT * DIPLOMA_PERCENT)
    advanced_count = int(STUDENTS_PER_DEPT * ADVANCED_PERCENT)
    bachelor_count = int(STUDENTS_PER_DEPT * BACHELOR_PERCENT)
    
    # Number of students with mixed courses
    mixed_courses_count = int(TOTAL_STUDENTS * MIXED_COURSES_PERCENT)
    
    # Collect course data for each department and level
    all_courses = {}
    for dept in DEPARTMENTS:
        all_courses[dept] = {}
        for level in ACADEMIC_LEVELS:
            all_courses[dept][level] = get_courses_by_department_level(connection, dept, level)
    
    # Generate regular students
    student_count = 0
    mixed_students = []
    
    for dept in DEPARTMENTS:
        # Student count for each academic level
        for level, count in [
            ("diploma", diploma_count), 
            ("advanced", advanced_count), 
            ("bachelor", bachelor_count)
        ]:
            for i in range(count):
                # Generate student ID
                student_id = f"{CURRENT_YEAR}{dept}{student_count+1:03d}"
                
                # Generate student details
                name = fake.name()
                email = f"{name.lower().replace(' ', '.')}@student.edu"
                phone = fake.phone_number()
                address = fake.address().replace('\n', ', ')
                
                # Determine if student has mixed courses
                has_mixed_courses = False
                if len(mixed_students) < mixed_courses_count and random.random() < 0.3:
                    has_mixed_courses = True
                    mixed_students.append({
                        "id": student_id,
                        "dept": dept,
                        "main_level": level
                    })
                
                # Insert student record
                cursor.execute("""
                INSERT INTO students (id, name, department, academic_level, email, phone, address, mixed_courses)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (student_id, name, dept, level, email, phone, address, has_mixed_courses))
                
                # Assign regular courses (from their academic level)
                if not has_mixed_courses:
                    available_courses = all_courses[dept][level]
                    if available_courses:
                        num_courses = random.randint(3, 6)  # Regular students take 3-6 courses
                        selected_courses = random.sample(available_courses, min(num_courses, len(available_courses)))
                        
                        for course_code in selected_courses:
                            cursor.execute("""
                            INSERT INTO enrollments (student_id, course_code)
                            VALUES (%s, %s)
                            """, (student_id, course_code))
                
                student_count += 1
    
    # Handle students with mixed courses
    for student in mixed_students:
        student_id = student["id"]
        dept = student["dept"]
        main_level = student["main_level"]
        
        # Determine the secondary level
        if main_level == "diploma":
            secondary_level = "advanced"
        elif main_level == "advanced":
            secondary_level = random.choice(["diploma", "bachelor"])
        else:  # Bachelor
            secondary_level = "advanced"
        
        # Get available courses
        primary_courses = all_courses[dept][main_level]
        secondary_courses = all_courses[dept][secondary_level]
        
        # Select courses (max 4 total for mixed students)
        primary_count = random.randint(1, 3)
        secondary_count = min(MAX_MIXED_COURSES - primary_count, 2)
        
        selected_primary = random.sample(primary_courses, min(primary_count, len(primary_courses)))
        selected_secondary = random.sample(secondary_courses, min(secondary_count, len(secondary_courses)))
        
        # Insert enrollment records
        for course_code in selected_primary + selected_secondary:
            cursor.execute("""
            INSERT INTO enrollments (student_id, course_code)
            VALUES (%s, %s)
            """, (student_id, course_code))
    
    connection.commit()
    cursor.close()
    print(f"Generated {student_count} students ({len(mixed_students)} with mixed courses)")

def main():
    try:
        # Connect to MySQL
        connection = mysql.connector.connect(**DB_CONFIG)
        print("Connected to MySQL server")
        
        # Create tables if they don't exist
        create_database_tables(connection)
        print("Student tables created successfully")
        
        # Generate and insert student data
        generate_students(connection)
        
        print("Student data generation completed successfully")
        connection.close()
        
    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
