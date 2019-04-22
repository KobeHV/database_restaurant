import pymysql
import traceback

db = pymysql.connect("localhost", "root", "123456", "restaurant", charset='utf8')


def insertORdelete(con, cursor, method, table, entry_values):#插入(method=1) 删除(method=0)
    if method == 1:
        sql = 'insert into ' + table + ' values (' + entry_values + ');'
        try:
            cursor.execute(sql)
            con.commit()
            return '插入成功'
        except Exception as e:
            error = repr(e)
            return error
    else:
        sql = 'delete from ' + table + ' where ' + entry_values + ';'
        try:
            cursor.execute(sql)
            con.commit()
            return '删除成功'
        except Exception as e:
            error = repr(e)
            return error


def connect_select(cursor, customerID):  # 连接查询
    sel_sql = '客户号,客户结账总单.店号,餐台点餐单.餐台号,餐台点餐单.点餐单号,' \
              '点餐单明细表.项目号,点餐单明细表.菜品号,菜品表.菜名,点餐单明细表.数量,菜品表.单价'
    from_sql = '客户结账总单 natural join 账单明细  natural join 餐台点餐单 natural join 点餐单明细表 natural join 菜品表'
    where_sql = ' 客户号 like "' + customerID + '";' # 不要写一些x.y = z.y 这样的语句
    sql = 'select ' + sel_sql + ' from ' + from_sql + ' where ' + where_sql
    cursor.execute(sql)
    return cursor.fetchall()

def nesting_select(cursor,descri,requir):   # 嵌套查询
    # 嵌套查询，查询格式如：查询符合descri要求的店的集合中工资最低/最高的员工、单价最高/最低的菜品
    sql=''
    if requir=='工资最低的员工':
        sql=' select 薪资,店表.店号,姓名,店名,地址 from 员工表 natural join 店表 ' \
            ' where 薪资<=all(select 薪资 from 员工表 where 员工表.店号 in(select 店号 from 店表 where '+descri+'))' \
            ' and 员工表.店号 in(select 店号 from 店表 where '+descri+');'
    elif requir=='工资最高的员工':
        sql=' select 薪资,店表.店号,姓名,店名,地址 from 员工表 natural join 店表 ' \
            ' where 薪资>=all(select 薪资 from 员工表 where 员工表.店号 in(select 店号 from 店表 where '+descri+'))' \
            ' and 员工表.店号 in(select 店号 from 店表 where '+descri+');'
    elif requir == '单价最低的菜品':
        sql = ' select 单价,菜品号,菜名,店表.店号,店名,地址 from 菜品表 natural  join 店表 ' \
              ' where 单价<=all(select 单价 from 菜品表 where 菜品表.店号 in(select 店号 from 店表 where ' + descri + '))'\
              ' and 菜品表.店号 in(select 店号 from 店表 where '+descri+');'
    elif requir == '单价最高的菜品':
        sql = ' select 单价,菜品号,菜名,店表.店号,店名,地址 from 菜品表 natural  join 店表 ' \
              ' where 单价>=all(select 单价 from 菜品表 where 菜品表.店号 in(select 店号 from 店表 where ' + descri + '))' \
              ' and 菜品表.店号 in(select 店号 from 店表 where ' + descri + ');'
    cursor.execute(sql)
    data=cursor.fetchall()
    return data

if __name__ == '__main__':
    con = pymysql.connect("localhost", "root", "123456", "restaurant", charset='utf8')
    cursor = con.cursor()

    message = insertORdelete(con, cursor, 1, '店表', "'5','小龙坎','四川成都','2010'")  # 菜品表每一项都不能为空，将一个属性改成null可以看到错误信息被返回
    print(message)
    # message = insertORdelete(con, cursor, 0, '店表', "店号=6")  # 菜品表每一项都不能为空，将一个属性改成null可以看到错误信息被返回
    # print(message)
    data = connect_select(cursor, '%238')
    print(data)
    data1=nesting_select(cursor,'地址 like "%济南%"','工资最高的员工')
    print(data1)
    print("\n")
    data2=nesting_select(cursor,'地址 like "%哈尔滨%"','工资最低的员工')
    print(data2)
    print("\n")
    data3=nesting_select(cursor,'地址 like "%杭州%"','单价最高的菜品')
    print(data3)
    print("\n")
    cursor.close()
    con.close()
