from pyspark import SparkContext, SQLContext

sc = SparkContext()
sql = SQLContext(sc)
df_dept = sql.read.schema("dept_id int,dept_name String").load(path="dept.csv", format='csv')
df_emp = sql.read.schema("emp_id int,emp_name String,gender String,emp_age int, emp_job"
                         " String, salary float, dept_id int").load(path="emp.csv", format='csv')
df_dept.registerTempTable('dept_table')
df_emp.registerTempTable('emp_table')
print('1.查询所有员工的基本信息，字段包括：部门名称，部门编号，员工编号，姓名，性别，年龄，岗位，薪水，先按部门编号排序，再按员工编号升序排序')
sql.sql('select d.dept_id,d.dept_name,e.emp_id,e.emp_name,e.gender,e.emp_age,e.emp_job,e.salary'
        ' from dept_table d join emp_table e on (d.dept_id =e.dept_id) order by d.dept_id asc, e.emp_id asc').show()
print('2.找出员工姓名是三个字的（模糊查询），并且工资在2000～3000范围内的员工信息，'
      '字段包括：部门名称，部门编号，员工编号，姓名，性别，年龄，岗位，薪水，按照薪水降序排序')
sql.sql('select d.dept_id,d.dept_name,e.emp_id,e.emp_name,e.gender,e.emp_age,e.emp_job,e.salary '
        'from dept_table d join emp_table e on (d.dept_id=e.dept_id) '
        'where e.emp_name like "___" and e.salary>=2000 and e.salary <=3000 order by e.salary desc').show()
print('3.找统计出个各部门的人数，平均工资，平均年龄，最高工资，最低工资'
      '字段包括：部门名称，部门编号，部门人数，平均年龄，最高工资，最低工资，按照部门排序')
sql.sql('select e.dept_id,d.dept_name,count(*),avg(e.emp_age),max(e.salary),avg(e.salary),min(e.salary) '
        'from dept_table d join emp_table e on (d.dept_id =e.dept_id) '
        'group by e.dept_id,d.dept_name order by e.dept_id ').show()
print('4.如果该公司年底会发一次年终奖，金额为6000元，请统计每个员工年收入（包括工资加年终奖）'
      '字段包括：部门名称，部门编号，员工编号，姓名，岗位，年收入，先按部门编号排序，再按员工编号升序排序')
sql.sql('select d.dept_id,d.dept_name,e.emp_id,e.emp_name,e.emp_job,e.salary*12+6000 as year_income '
        'from dept_table d join emp_table e on (d.dept_id =e.dept_id) order by d.dept_id,e.emp_id ').show()
print('5.找出所有薪资大于等于本部门平均工资的员工，'
      '字段包括：部门名称，部门编号，部门平均工资，员工编号，姓名，岗位，薪资，先按部门编号升序排序，再按薪资降序排序')
sql.sql('select e.dept_id,dept_table.dept_name, avg_salary,e.emp_id,e.emp_name, e.emp_job,e.salary '
        'from emp_table e join '
        '(select avg(salary) as avg_salary,dept_id from emp_table group by dept_id) d on (e.dept_id=d.dept_id)'
        'join dept_table on (dept_table.dept_id=e.dept_id)'
        'where salary>=avg_salary order by e.dept_id,e.salary desc').show()