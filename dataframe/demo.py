from pyspark.sql import functions as F
from pyspark import SparkContext, SQLContext

sc = SparkContext()
sqlContext = SQLContext(sc)
dfr = sqlContext.read
df_dept = dfr.schema("dept_id int,dept_name String").csv(path="dept.csv")
df_emp = dfr.schema("emp_id int,emp_name String,gender String,emp_age int, emp_job"
                    " String, salary float, dept_id int").csv(path="emp.csv")
print('1.查询所有员工的基本信息，字段包括：部门名称，部门编号，员工编号，姓名，性别，年龄，岗位，薪水，先按部门编号排序，再按员工编号升序排序')
emp_join_dept = df_emp.join(df_dept, on='dept_id')  # emp join dept 关联 dept_id 字段
emp_join_dept.sort('dept_id', 'emp_id').show()  # 部门编号升序
print('2.找出员工姓名是三个字的（模糊查询），并且工资在2000～3000范围内的员工信息，'
      '字段包括：部门名称，部门编号，员工编号，姓名，性别，年龄，岗位，薪水，按照薪水降序排序')
emp_join_dept.filter("emp_name like ('___') and salary >= 2000 and salary <= 3000") \
    .sort('salary', ascending=False).show()  # 员工姓名是三个字, 工资大于等于2000，小于等于3000，薪资降序
print('3.找统计出个各部门的人数，平均工资，平均年龄，最高工资，最低工资'
      '字段包括：部门名称，部门编号，部门人数，平均年龄，最高工资，最低工资，按照部门排序')
emp_join_dept.groupBy('dept_id', 'dept_name').agg(
    F.min(emp_join_dept.salary),
    F.max(emp_join_dept.salary),
    F.count(emp_join_dept.emp_id),
    F.avg(emp_join_dept.emp_age),
    F.avg(emp_join_dept.salary)).sort('dept_id').show()
print('4.如果该公司年底会发一次年终奖，金额为6000元，请统计每个员工年收入（包括工资加年终奖）'
      '字段包括：部门名称，部门编号，员工编号，姓名，岗位，年收入，先按部门编号排序，再按员工编号升序排序')
emp_join_dept.select('dept_id', 'dept_name', 'emp_id', 'emp_name', 'emp_job',
                     (emp_join_dept.salary * 12 + 6000).alias('annual_income')).sort('dept_id', 'emp_id').show()
print('5.找出所有薪资大于等于本部门平均工资的员工，'
      '字段包括：部门名称，部门编号，部门平均工资，员工编号，姓名，岗位，薪资，先按部门编号升序排序，再按薪资降序排序')
salary_avg = emp_join_dept.groupBy('dept_id', 'dept_name').agg(
    F.avg(emp_join_dept.salary).alias('avg_salary'))  # 先分组统计查询各部门的平均工资，且重命名结果字段为avg_salary
salary_avg_join_emp = df_emp.join(salary_avg, on='dept_id')  # 将分组统计平均工资结果与员工表关联
salary_avg_join_emp.select('dept_id', 'dept_name', 'avg_salary', 'emp_id', 'emp_name', 'emp_job', 'salary') \
    .filter(df_emp.salary >= salary_avg.avg_salary).orderBy(df_emp.dept_id.asc(),
                                                            df_emp.salary.desc()).show()  # 过滤salary大于avg_salary的员工信息，使用orderBy对结果进行多字段排序

exit()

print('1.查处该单位当前设置有那些岗位，按照岗位名称降序排序')
df_emp.select('emp_job').distinct().sort('emp_job', ascending=False).show(vertical=False)  # 岗位名降序
print('2.查询所有员工的基本信息，字段包括：部门名称，部门编号，员工编号，姓名，性别，年龄，岗位，薪水，先按部门编号排序，再按员工编号升序排序')
emp_join_dept = df_emp.join(df_dept, on='dept_id')  # emp join dept 关联 dept_id 字段
emp_join_dept.sort('dept_id', 'emp_id').show()  # 部门编号升序
print('3.找出所有姓刘的员工信息，字段包括：部门名称，部门编号，员工编号，姓名，性别，年龄，岗位，薪水，先按部门编号排序，再按员工编号升序排序')
emp_join_dept.filter(emp_join_dept.emp_name.like('刘%')).sort('dept_id', 'emp_id').show()  # 所有姓刘员工，部门编号升序
print('4.找出年龄在25岁以下，并且工资在2000～3000范围内的员工信息，字段包括：部门名称，部门编号，员工编号，姓名，性别，年龄，岗位，薪水，按照薪水降序排序')
emp_join_dept.filter("emp_age < 25 and salary >= 2000 and salary <= 3000") \
    .sort('salary', ascending=False).show()  # 年龄小于25, 工资大于等于2000，小于等于3000，薪资降序
print('5.统计出个各部门的平均工资，平均年龄，字段包括：部门名称，部门编号，平均年龄，平均工资，按部门编号排序')
emp_join_dept.groupBy('dept_id', 'dept_name').avg('emp_age', 'salary').sort('dept_id').show()
