"""
Coze Database Middleware
中间件服务 - 连接 Coze 智能体与 MySQL 数据库
"""
import os
import time
from typing import Optional, Tuple, Union
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv
import pymysql
from pymysql.cursors import DictCursor

# 加载环境变量
load_dotenv()

# 环境变量读取
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
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
    api_key = request.headers.get("X-API-Key")
    
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


def get_db_connection() -> Tuple[bool, Union[pymysql.connections.Connection, dict]]:
    """
    创建数据库连接
    
    Returns:
        Tuple[bool, Union[Connection, dict]]: 
            - (True, connection) 如果连接成功
            - (False, error_response) 如果连接失败
    
    Requirements: 2.1, 2.2, 2.3, 2.4
    """
    # 检查数据库配置是否完整 (Requirements 2.1, 2.2)
    required_vars = {
        "DB_HOST": localhost,
        "DB_USER": root,
        "DB_PASSWORD": 825316,
        "DB_NAME": mini_ecommerce
    }
    
    missing_vars = [name for name, value in required_vars.items() if not value]
    
    if missing_vars:
        return False, {
            "success": False,
            "error": "ConfigurationError",
            "message": f"数据库配置缺失: {', '.join(missing_vars)}"
        }
    
    # 尝试连接数据库 (Requirements 2.3, 2.4)
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            port=int(DB_PORT),
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset='utf8mb4',
            cursorclass=DictCursor,
            connect_timeout=10
        )
        return True, connection
    except pymysql.err.OperationalError as e:
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


@app.get("/")
async def health_check():
    """
    健康检查端点
    Requirements: 4.2
    """
    return {
        "success": True,
        "message": "Coze Database Middleware is running",
        "version": "1.0.0"
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
            detail={
                "success": False,
                "error": "ValidationError",
                "message": "SQL 语句缺失或为空"
            }
        )
    
    # 获取数据库连接
    success, result = get_db_connection()
    if not success:
        raise HTTPException(status_code=500, detail=result)
    
    connection = result
    
    try:
        with connection.cursor() as cursor:
            # 执行 SQL 语句 (Requirements 1.1)
            cursor.execute(request.sql)
            
            # 判断 SQL 类型并处理结果
            if is_select_query(request.sql):
                # SELECT 查询 - 返回结果集 (Requirements 1.4, 3.1)
                data = cursor.fetchall()
                connection.commit()
                
                execution_time = time.time() - start_time
                
                return {
                    "success": True,
                    "data": data,
                    "message": "查询成功",
                    "rows_affected": len(data),
                    "execution_time": execution_time
                }
            else:
                # INSERT/UPDATE/DELETE - 返回影响行数 (Requirements 3.2, 3.3, 3.4)
                rows_affected = cursor.rowcount
                connection.commit()
                
                execution_time = time.time() - start_time
                
                return {
                    "success": True,
                    "data": None,
                    "message": "操作成功",
                    "rows_affected": rows_affected,
                    "execution_time": execution_time
                }
                
    except pymysql.err.ProgrammingError as e:
      
