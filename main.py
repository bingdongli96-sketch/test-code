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
        "DB_HOST": DB_HOST,
        "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASSWORD,
        "DB_NAME": DB_NAME
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
