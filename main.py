"""
Coze Database Middleware
中间件服务 - 连接 Coze 智能体与 MySQL 数据库
"""
import os
import time
import logging
from typing import Optional, Tuple, Union
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

# 配置日志记录 (Requirements 6.1, 6.2, 6.3)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv()

# 环境变量读取
DB_HOST = os.getenv("DB_HOST", "36.138.184.180")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "libd")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Libd@123")
DB_NAME = os.getenv("DB_NAME", "test")
API_KEY = os.getenv("API_KEY")

# 初始化 FastAPI 应用
app = FastAPI(
    title="Coze Database Middleware",
    description="连接 Coze 智能体与 MySQL 数据库的中间件服务",
    version="1.0.0"
)

# 配置 CORS 中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 请求日志记录中间件 (Requirements 6.1, 6.2, 6.3)
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """
    请求日志记录中间件
    - 记录请求时间和请求路径 (Requirements 6.1)
    - 记录执行耗时 (Requirements 6.2)
    - 记录错误详情 (Requirements 6.3)
    """
    import datetime
    
    # 记录请求开始时间
    start_time = time.time()
    request_time = datetime.datetime.now().isoformat()
    
    # 记录请求信息 (Requirements 6.1)
    logger.info(f"请求开始 - 时间: {request_time}, 路径: {request.url.path}, 方法: {request.method}")
    
    try:
        # 处理请求
        response = await call_next(request)
        
        # 计算执行耗时 (Requirements 6.2)
        execution_time = time.time() - start_time
        logger.info(f"请求完成 - 路径: {request.url.path}, 状态码: {response.status_code}, 耗时: {execution_time:.4f}秒")
        
        return response
    except Exception as e:
        # 记录错误详情 (Requirements 6.3)
        execution_time = time.time() - start_time
        logger.error(f"请求错误 - 路径: {request.url.path}, 错误: {str(e)}, 耗时: {execution_time:.4f}秒")
        raise


# API Key 验证中间件
@app.middleware("http")
async def verify_api_key(request: Request, call_next):
    """
    验证 API Key 中间件
    - 健康检查端点 "/" 跳过验证
    - 检查 X-API-Key 请求头
    - 返回 401 如果 API Key 缺失
    - 返回 403 如果 API Key 不匹配
    """
    # 健康检查端点跳过验证 (Requirements 7.5)
    if request.url.path == "/":
        return await call_next(request)
    
    # 检查 X-API-Key 请求头 (Requirements 7.1)
    api_key = request.headers.get("API_Key")
    
    # API Key 缺失返回 401 (Requirements 7.2)
    if not api_key:
        return JSONResponse(
            status_code=401,
            content={
                "success": False,
                "error": "AuthenticationError",
                "message": "API Key 缺失，请在请求头中提供 X-API-Key"
            }
        )
    
    # API Key 不匹配返回 403 (Requirements 7.3)
    if api_key != API_KEY:
        return JSONResponse(
            status_code=403,
            content={
                "success": False,
                "error": "AuthorizationError",
                "message": "API Key 无效，授权失败"
            }
        )
    
    # API Key 验证通过，继续处理请求 (Requirements 7.4)
    return await call_next(request)


# 请求模型
class QueryRequest(BaseModel):
    sql: str


# 响应模型
class QueryResponse(BaseModel):
    success: bool
    data: Optional[list] = None
    message: str
    rows_affected: int = 0
    execution_time: float = 0.0


class ErrorResponse(BaseModel):
    success: bool = False
    error: str
    message: str


def get_db_connection() -> Tuple[bool, Union[psycopg2.extensions.connection, dict]]:
    """
    创建数据库连接
    
    Returns:
        Tuple[bool, Union[Connection, dict]]: 
            - (True, connection) 如果连接成功
            - (False, error_response) 如果连接失败
    
    Requirements: 2.1, 2.2, 2.3, 2.4
    """
    # 数据库配置
    db_host = DB_HOST or "36.138.184.180"
    db_port = int(DB_PORT) if DB_PORT else 5432
    db_user = DB_USER or "libd"
    db_password = DB_PASSWORD or "Libd@123"
    db_name = DB_NAME or "test"
    
    # 尝试连接数据库 (Requirements 2.3, 2.4)
    try:
        connection = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name,
            connect_timeout=10
        )
        return True, connection
    except psycopg2.OperationalError as e:
        return False, {
            "success": False,
            "error": "ConnectionError",
            "message": f"数据库连接失败: {str(e)}"
        }
    except Exception as e:
        return False, {
            "success": False,
            "error": "ConnectionError",
            "message": f"数据库连接异常: {str(e)}"
        }


def is_select_query(sql: str) -> bool:
    """
    判断 SQL 语句是否为 SELECT 查询
    
    Args:
        sql: SQL 语句
        
    Returns:
        bool: True 如果是 SELECT 查询，否则 False
    """
    sql_upper = sql.strip().upper()
    return sql_upper.startswith("SELECT")


def serialize_value(value):
    """
    将数据库值转换为 JSON 可序列化的格式
    
    处理特殊类型：
    - datetime -> ISO 格式字符串
    - date -> ISO 格式字符串
    - time -> 字符串格式
    - timedelta -> 总秒数字符串
    - Decimal -> float
    - bytes -> base64 编码字符串
    
    Args:
        value: 数据库返回的值
        
    Returns:
        JSON 可序列化的值
        
    Requirements: 5.3, 5.4
    """
    import datetime
    from decimal import Decimal
    import base64
    
    if value is None:
        return None
    elif isinstance(value, datetime.datetime):
        return value.isoformat()
    elif isinstance(value, datetime.date):
        return value.isoformat()
    elif isinstance(value, datetime.time):
        return value.isoformat()
    elif isinstance(value, datetime.timedelta):
        return str(value)
    elif isinstance(value, Decimal):
        return float(value)
    elif isinstance(value, bytes):
        return base64.b64encode(value).decode('utf-8')
    else:
        return value


def format_row(row: dict) -> dict:
    """
    格式化单行数据，将所有值转换为 JSON 可序列化格式
    
    Args:
        row: 数据库返回的行数据（字典格式）
        
    Returns:
        格式化后的行数据
        
    Requirements: 5.3, 5.4
    """
    return {key: serialize_value(value) for key, value in row.items()}


def format_response_data(data: list) -> list:
    """
    格式化查询结果数据，确保所有数据可被 JSON 序列化
    
    Args:
        data: 数据库查询结果列表
        
    Returns:
        格式化后的数据列表
        
    Requirements: 5.3, 5.4
    """
    if not data:
        return []
    return [format_row(row) for row in data]


def build_success_response(data: Optional[list], message: str, rows_affected: int, execution_time: float) -> dict:
    """
    构建标准成功响应结构
    
    Args:
        data: 查询结果数据
        message: 响应消息
        rows_affected: 影响的行数
        execution_time: 执行耗时（秒）
        
    Returns:
        标准 JSON 响应结构
        
    Requirements: 5.1
    """
    return {
        "success": True,
        "data": format_response_data(data) if data else None,
        "message": message,
        "rows_affected": rows_affected,
        "execution_time": round(execution_time, 6)
    }


def build_error_response(error_type: str, message: str) -> dict:
    """
    构建标准错误响应结构
    
    Args:
        error_type: 错误类型
        message: 错误详情
        
    Returns:
        标准错误响应结构
        
    Requirements: 5.2
    """
    return {
        "success": False,
        "error": error_type,
        "message": message
    }


@app.get("/")
async def health_check():
    """
    健康检查端点
    返回服务状态信息
    Requirements: 4.2
    """
    import datetime
    return {
        "success": True,
        "message": "Coze Database Middleware is running",
        "version": "1.0.0",
        "status": "healthy",
        "timestamp": datetime.datetime.now().isoformat()
    }


@app.post("/query")
async def execute_query(request: QueryRequest):
    """
    执行 SQL 查询端点
    
    接收 POST 请求和 SQL 语句，判断 SQL 类型，执行查询并返回结果
    
    Requirements: 1.1, 1.2, 1.3, 1.4, 3.1, 3.2, 3.3, 3.4, 3.5
    """
    start_time = time.time()
    
    # 验证 SQL 语句是否存在 (Requirements 1.2)
    if not request.sql or not request.sql.strip():
        raise HTTPException(
            status_code=400,
            detail=build_error_response("ValidationError", "SQL 语句缺失或为空")
        )
    
    # 获取数据库连接
    success, result = get_db_connection()
    if not success:
        raise HTTPException(status_code=500, detail=result)
    
    connection = result
    
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # 执行 SQL 语句 (Requirements 1.1)
            cursor.execute(request.sql)
            
            execution_time = time.time() - start_time
            
            # 判断 SQL 类型并处理结果
            if is_select_query(request.sql):
                # SELECT 查询 - 返回结果集 (Requirements 1.4, 3.1)
                data = cursor.fetchall()
                # 将 RealDictRow 转换为普通字典
                data = [dict(row) for row in data]
                connection.commit()
                
                # 使用格式化函数构建响应 (Requirements 5.1, 5.3, 5.4)
                return build_success_response(
                    data=data,
                    message="查询成功",
                    rows_affected=len(data),
                    execution_time=execution_time
                )
            else:
                # INSERT/UPDATE/DELETE - 返回影响行数 (Requirements 3.2, 3.3, 3.4)
                rows_affected = cursor.rowcount
                connection.commit()
                
                # 使用格式化函数构建响应 (Requirements 5.1)
                return build_success_response(
                    data=None,
                    message="操作成功",
                    rows_affected=rows_affected,
                    execution_time=execution_time
                )
                
    except psycopg2.errors.SyntaxError as e:
        # SQL 语法错误 (Requirements 3.5)
        connection.rollback()
        raise HTTPException(
            status_code=400,
            detail=build_error_response("SQLError", f"SQL 执行失败: {str(e)}")
        )
    except psycopg2.OperationalError as e:
        connection.rollback()
        raise HTTPException(
            status_code=400,
            detail=build_error_response("SQLError", f"SQL 执行失败: {str(e)}")
        )
    except Exception as e:
        connection.rollback()
        raise HTTPException(
            status_code=500,
            detail=build_error_response("InternalError", f"内部错误: {str(e)}")
        )
    finally:
        connection.close()
