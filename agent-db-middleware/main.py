"""
智能体-数据库中间件核心
功能：接收智能体语义请求 → 解析为SQL → 执行数据库查询 → 返回结构化结果
适配：Vercel Serverless、跨域、环境变量配置
"""
from flask import Flask, request, jsonify
from flask_cors import CORS  # 处理跨域（插件调用必备）
import pymysql
import re
import os
import datetime

# ========== 初始化Flask应用（Vercel要求实例名为app） ==========
app = Flask(__name__)
CORS(app)  # 允许所有跨域请求（插件调用需跨域）

# ========== 数据库配置（从Vercel环境变量读取，安全且适配部署） ==========
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),        # 云数据库公网地址（如rm-xxxx.mysql.rds.aliyuncs.com）
    "user": os.getenv("DB_USER"),        # 数据库用户名
    "password": os.getenv("DB_PASSWORD"),# 数据库密码
    "database": os.getenv("DB_NAME", "test_db"),  # 数据库名（默认test_db）
    "charset": "utf8mb4",
    "connect_timeout": 10  # 超时时间（适配Vercel海外节点）
}

# ========== 核心功能1：自然语言转SQL（规则引擎版，可扩展为LLM） ==========
def parse_nl_to_sql(nl_text: str) -> str:
    """
    解析智能体的自然语言请求为MySQL查询SQL
    支持的语义类型：
    - 按地区查询：如"查询长春的项目数据"
    - 按年份查询：如"查询2025年的项目"、"查询近3年的机器学习项目"
    - 按项目类型查询：如"查询机器学习项目"
    - 组合条件：如"查询长春地区近3年的机器学习项目数据"
    """
    # 基础表结构（可根据你的实际表修改）：project(id, name, area, type, year, leader)
    table_name = "project"
    
    # 关键词提取规则
    area_pattern = re.compile(r"([省市县区]+)地区|([省市县区]+)")  # 匹配地区
    year_pattern = re.compile(r"(\d{4})年|近(\d+)年")              # 匹配年份/近N年
    type_pattern = re.compile(r"([\u4e00-\u9fa5]+)项目|([\u4e00-\u9fa5]+)类型")  # 匹配项目类型

    # 初始化查询条件
    conditions = []
    
    # 1. 提取地区
    area_match = area_pattern.search(nl_text)
    if area_match:
        area = area_match.group(1) or area_match.group(2)
        conditions.append(f"area = '{area}'")
    
    # 2. 提取年份
    year_match = year_pattern.search(nl_text)
    if year_match:
        if year_match.group(1):  # 具体年份（如2025年）
            year = year_match.group(1)
            conditions.append(f"year = {year}")
        elif year_match.group(2):  # 近N年（如近3年）
            current_year = datetime.datetime.now().year
            n_years = int(year_match.group(2))
            start_year = current_year - n_years + 1
            conditions.append(f"year >= {start_year}")
    
    # 3. 提取项目类型
    type_match = type_pattern.search(nl_text)
    if type_match:
        project_type = type_match.group(1) or type_match.group(2)
        conditions.append(f"type = '{project_type}'")

    # 拼接SQL（无条件则查询全部）
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    sql = f"SELECT * FROM {table_name} WHERE {where_clause};"
    return sql

# ========== 核心功能2：数据库执行（含异常处理） ==========
def execute_mysql_sql(sql: str) -> tuple:
    """
    执行SQL并返回结果
    返回格式：(是否成功, 结果数据, 提示信息)
    """
    conn = None
    cursor = None
    try:
        # 连接数据库（适配Vercel海外节点，增加超时处理）
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor(pymysql.cursors.DictCursor)  # 返回字典格式（易序列化）
        
        # 执行SQL
        cursor.execute(sql)
        results = cursor.fetchall()
        
        # 处理结果（转为列表+字典，JSON友好）
        processed_results = [dict(row) for row in results]
        return True, processed_results, f"查询成功，共返回{len(processed_results)}条数据"
    
    except pymysql.MySQLError as e:
        error_msg = f"数据库错误：{e.args[1]}（错误码：{e.args[0]}）"
        return False, [], error_msg
    except Exception as e:
        error_msg = f"执行失败：{str(e)}"
        return False, [], error_msg
    finally:
        # 确保关闭连接（Serverless环境必须释放资源）
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ========== 核心API：供智能体/插件调用 ==========
@app.route('/api/process-request', methods=['POST'])
def process_agent_request():
    """
    智能体请求处理接口（插件调用的核心接口）
    请求格式：JSON {"nl_request": "自然语言请求文本"}
    返回格式：JSON（含SQL、结果、状态）
    """
    # 1. 校验请求格式
    if not request.is_json:
        return jsonify({
            "success": False,
            "sql": "",
            "results": [],
            "message": "请求格式错误，需为JSON格式"
        }), 400
    
    # 2. 获取请求参数
    req_data = request.get_json()
    nl_request = req_data.get("nl_request", "").strip()
    if not nl_request:
        return jsonify({
            "success": False,
            "sql": "",
            "results": [],
            "message": "请输入有效的自然语言请求"
        }), 400
    
    # 3. 解析为SQL
    sql = parse_nl_to_sql(nl_request)
    
    # 4. 执行SQL
    exec_success, exec_results, exec_msg = execute_mysql_sql(sql)
    
    # 5. 返回结果（标准化格式，方便插件解析）
    return jsonify({
        "success": exec_success,
        "sql": sql,          # 返回解析后的SQL（便于调试）
        "results": exec_results,  # 数据库返回的结构化数据
        "message": exec_msg  # 提示信息（成功/失败原因）
    })

# ========== Vercel Serverless 适配入口（必须） ==========
def handler(environ, start_response):
    """Vercel Serverless 启动入口"""
    return app(environ, start_response)

# ========== 本地测试入口（本地运行时用，Vercel部署时自动忽略） ==========
if __name__ == "__main__":
    # 本地运行时初始化测试数据库（首次运行）
    try:
        test_conn = pymysql.connect(
            host=DB_CONFIG["host"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"]
        )
        test_cursor = test_conn.cursor()
        # 创建数据库
        test_cursor.execute("CREATE DATABASE IF NOT EXISTS test_db;")
        test_cursor.execute("USE test_db;")
        # 创建测试表
        test_cursor.execute("""
            CREATE TABLE IF NOT EXISTS project (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL COMMENT '项目名称',
                area VARCHAR(50) NOT NULL COMMENT '地区',
                type VARCHAR(50) NOT NULL COMMENT '项目类型',
                year INT NOT NULL COMMENT '年份',
                leader VARCHAR(50) NOT NULL COMMENT '负责人'
            ) COMMENT '测试项目表';
        """)
        # 插入测试数据（避免重复插入）
        test_cursor.execute("""
            INSERT IGNORE INTO project (name, area, type, year, leader) VALUES
            ('智能推荐系统', '长春', '机器学习', 2023, '张三'),
            ('图像识别平台', '长春', '计算机视觉', 2024, '李四'),
            ('大模型训练', '北京', '人工智能', 2025, '王五'),
            ('数据挖掘分析', '长春', '机器学习', 2025, '赵六');
        """)
        test_conn.commit()
        test_cursor.close()
        test_conn.close()
        print("本地测试数据库初始化成功！")
    except Exception as e:
        print(f"本地数据库初始化失败：{str(e)}")
    
    # 启动本地服务
    app.run(debug=True, host="0.0.0.0", port=5000)