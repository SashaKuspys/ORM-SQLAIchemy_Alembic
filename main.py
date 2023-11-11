from sqlalchemy import func, desc, select, and_, text

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session


def select_01():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 5;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result


def select_02():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    where g.subject_id = 1
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 1;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subjects_id == 1).group_by(Student.id).order_by(
        desc('average_grade')).limit(1).all()
    return result


def select_03():
    """
    SELECT
        g.subject_id,
        s.group_id,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON g.student_id = s.id
    WHERE g.subject_id = 1
    GROUP BY g.subject_id, s.group_id;
    """
    result = (
        session.query(
            Grade.subjects_id,
            Student.group_id,
            func.round(func.avg(Grade.grade), 2).label('average_grade')
        )
        .join(Student, Grade.student_id == Student.id)
        .filter(Grade.subjects_id == 1)
        .group_by(Grade.subjects_id, Student.group_id)
        .all()
    )
    return result


def select_04():
    """
    SELECT
        ROUND(AVG(grade), 2) AS average_grade
    FROM grades;
    """
    result = (
        session.query(func.round(func.avg(Grade.grade), 2).label('average_grade'))
        .from_statement(text('SELECT ROUND(AVG(grade), 2) AS average_grade FROM grades'))
        .all()
    )
    return result


def select_05():
    """
    SELECT
        s.name AS course_name
    FROM subjects s
    WHERE s.teacher_id = 1;
    """
    result = (
        session.query(Subject.name.label('course_name'))
        .filter(Subject.teacher_id == 1)
        .all()
    )
    return result


def select_06():
    """
    SELECT
        id,
        fullname
    FROM students
    WHERE group_id = 1;
    """
    result = (
        session.query(Student.id, Student.fullname)
        .filter(Student.group_id == 1)
        .all()
    )
    return result


def select_07():
    """
    SELECT
        s.fullname AS student_name,
        g.grade,
        sub.name AS subject_name
    FROM students s
    JOIN grades g ON s.id = g.student_id
    JOIN subjects sub ON g.subjects_id = sub.id
    WHERE s.group_id = 1
    AND g.subjects_id = 2;
    """
    result = (
        session.query(
            Student.fullname.label('student_name'),
            Grade.grade,
            Subject.name.label('subject_name')
        )
        .join(Grade, Student.id == Grade.student_id)
        .join(Subject, Grade.subjects_id == Subject.id)
        .filter(Student.group_id == 1, Grade.subjects_id == 2)
        .all()
    )
    return result


def select_08():
    """
    SELECT
        t.fullname AS teacher_name,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM teachers t
    JOIN subjects sub ON t.id = sub.teacher_id
    JOIN grades g ON sub.id = g.subjects_id
    WHERE t.id = 1
    GROUP BY t.fullname;
    """
    result = (
        session.query(
            Teacher.fullname.label('teacher_name'),
            func.round(func.avg(Grade.grade), 2).label('average_grade')
        )
        .join(Subject, Teacher.id == Subject.teacher_id)
        .join(Grade, Subject.id == Grade.subjects_id)
        .filter(Teacher.id == 1)
        .group_by(Teacher.fullname)
        .all()
    )
    return result


def select_09():
    """
    SELECT
        sub.name AS course_name
    FROM students s
    JOIN grades g ON s.id = g.student_id
    JOIN subjects sub ON g.subjects_id = sub.id
    WHERE s.id = 1;
    """
    result = (
        session.query(Subject.name.label('course_name'))
        .join(Grade, Grade.subjects_id == Subject.id)
        .join(Student, Student.id == Grade.student_id)
        .filter(Student.id == 1)
        .all()
    )
    return result


def select_10():
    """
    SELECT
        sub.name AS course_name
    FROM students s
    JOIN grades g ON s.id = g.student_id
    JOIN subjects sub ON g.subjects_id = sub.id
    JOIN teachers t ON sub.teacher_id = t.id
    WHERE s.id = 1
        AND t.id = 2;
    """
    result = (
        session.query(Subject.name.label('course_name'))
        .join(Grade, Subject.id == Grade.subjects_id)
        .join(Student, Grade.student_id == Student.id)
        .join(Teacher, Subject.teacher_id == Teacher.id)
        .filter(Student.id == 1, Teacher.id == 2)
        .all()
    )
    return result


def select_11():
    """
    SELECT
        t.fullname AS teacher_name,
        s.fullname AS student_name,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM teachers t
    JOIN subjects sub ON t.id = sub.teacher_id
    JOIN grades g ON sub.id = g.subject_id
    JOIN students s ON g.student_id = s.id
    WHERE t.id = 1
        AND s.id = 2
    GROUP BY t.fullname, s.fullname;
    """
    result = (
        session.query(
            Teacher.fullname.label('teacher_name'),
            Student.fullname.label('student_name'),
            func.round(func.avg(Grade.grade), 2).label('average_grade')
        )
        .join(Subject, Teacher.id == Subject.teacher_id)
        .join(Grade, Subject.id == Grade.subjects_id)
        .join(Student, Grade.student_id == Student.id)
        .filter(Teacher.id == 1, Student.id == 2)
        .group_by(Teacher.fullname, Student.fullname)
        .all()
    )
    return result


def select_12():
    """
    select max(grade_date)
    from grades g
    join students s on s.id = g.student_id
    where g.subject_id = 2 and s.group_id  =3;

    select s.id, s.fullname, g.grade, g.grade_date
    from grades g
    join students s on g.student_id = s.id
    where g.subject_id = 2 and s.group_id = 3 and g.grade_date = (
        select max(grade_date)
        from grades g2
        join students s2 on s2.id=g2.student_id
        where g2.subject_id = 2 and s2.group_id = 3
    );
    :return:
    """

    subquery = (select(func.max(Grade.grade_date)).join(Student).filter(and_(
        Grade.subjects_id == 2, Student.group_id == 3
    ))).scalar_subquery()

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.grade_date) \
        .select_from(Grade) \
        .join(Student) \
        .filter(and_(Grade.subjects_id == 2, Student.group_id == 3, Grade.grade_date == subquery)).all()

    return result


if __name__ == '__main__':
    # print(select_01())
    # print(select_02())
    # print(select_03())
    # print(select_04())
    # print(select_05())
    # print(select_06())
    # print(select_07())
    # print(select_08())
    # print(select_09())
    # print(select_10())
    # print(select_11())
    print(select_12())