from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from models import QueryRequest
from db import list_collections, get_collection, sample_fields
from utils import flatten_doc
from prefs import PrefsStore
import pandas as pd
from io import BytesIO
import chardet
import math
import csv
import io

# 工具函数：将值转换为JSON安全的格式
def safe_convert_value(v):
    """Convert value to JSON-safe format"""
    if v is None:
        return None
    if isinstance(v, (int, str, bool)):
        return v
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    return str(v)

app=FastAPI(title="Mongo Pivot API (IFERROR Global • Fix DIV v3 + Upload • repack)")
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_headers=["*"],allow_methods=["*"])
prefs=PrefsStore()

@app.get("/")
def root(): return {"message":"ok"}

@app.get("/api/health")
def health(): return {"status":"ok"}

@app.get("/api/collections")
def collections(): return {"collections": list_collections()}

@app.get("/api/fields")
def fields(collection:str, sample:int=200): 
    return {"collection":collection,"fields": sample_fields(collection,sample)}

@app.get("/api/peek")
def peek(collection:str, limit:int=50):
    coll=get_collection(collection)
    rows=[]
    for doc in coll.find({}, {"_id":0}).limit(limit):
        safe_doc = {k: safe_convert_value(v) for k,v in doc.items()}
        rows.append(flatten_doc(safe_doc))
    return {"count":len(rows),"rows":rows}

@app.post("/api/query")
def query(req: QueryRequest):
    coll=get_collection(req.collection)
    if not isinstance(req.filters, dict):
        raise HTTPException(400, "filters must be object")
    cur=coll.find(req.filters, req.projection).skip(req.skip).limit(req.limit)
    rows=[]
    for doc in cur:
        safe_doc = {k:(str(v) if k=="_id" else safe_convert_value(v)) for k,v in doc.items()}
        rows.append(flatten_doc(safe_doc))
    return {"count":len(rows),"rows":rows}

def detect_separator(content, encoding):
    """
    智能检测 CSV 分隔符
    """
    try:
        # 解码样本数据
        sample_size = min(4096, len(content))  # 使用更大的样本
        sample = content[:sample_size].decode(encoding, errors='ignore')
        
        # 获取前几行用于分析
        lines = sample.split('\n')[:5]  # 分析前5行
        valid_lines = [line.strip() for line in lines if line.strip()]
        
        if not valid_lines:
            return ','
        
        # 统计各种分隔符的出现次数
        separators = [',', '\t', ';', '|']
        separator_scores = {}
        
        for sep in separators:
            scores = []
            for line in valid_lines:
                count = line.count(sep)
                scores.append(count)
            
            # 计算一致性分数：平均值高且方差小的分隔符越好
            if scores and max(scores) > 0:
                avg_count = sum(scores) / len(scores)
                consistency = 1.0 / (1.0 + (max(scores) - min(scores)))  # 一致性越高分数越高
                separator_scores[sep] = avg_count * consistency
            else:
                separator_scores[sep] = 0
        
        # 选择得分最高的分隔符
        best_sep = max(separator_scores.items(), key=lambda x: x[1])[0]
        
        # 如果所有分隔符得分都太低，默认使用逗号
        if separator_scores[best_sep] < 0.5:
            return ','
            
        return best_sep
        
    except Exception:
        return ','

def parse_csv_robust(content, encoding, sep, start_row):
    """
    强大的CSV解析函数，支持多种解析策略
    """
    df = None
    errors = []
    
    # 策略1: 标准pandas解析
    try:
        df = pd.read_csv(
            BytesIO(content),
            sep=sep,
            header=start_row-1 if start_row > 1 else 0,
            encoding=encoding,
            engine='python',
            skipinitialspace=True,
            dtype=str,
            na_values=['', 'NULL', 'null', 'N/A', 'n/a', 'NA', 'na'],
            keep_default_na=True
        )
        # 检查是否解析成功（列名不应该是一个长字符串）
        if len(df.columns) > 1 or not any(',' in str(col) or '\t' in str(col) or ';' in str(col) for col in df.columns):
            return df
        else:
            errors.append("标准解析失败: 列名解析不正确")
    except Exception as e:
        errors.append(f"标准解析失败: {str(e)}")
    
    # 策略2: 宽松的pandas解析 - 忽略引号问题
    try:
        df = pd.read_csv(
            BytesIO(content),
            sep=sep,
            header=start_row-1 if start_row > 1 else 0,
            encoding=encoding,
            engine='python',
            skipinitialspace=True,
            dtype=str,
            quoting=3,  # QUOTE_NONE - 不处理引号
            na_values=['', 'NULL', 'null', 'N/A', 'n/a', 'NA', 'na'],
            keep_default_na=True
        )
        return df
    except Exception as e:
        errors.append(f"宽松解析失败: {str(e)}")
    
    # 策略3: 使用csv模块手动解析
    try:
        text_content = content.decode(encoding, errors='replace')
        
        # 创建自定义方言
        class FlexibleDialect(csv.excel):
            def __init__(self, delimiter=','):
                super().__init__()
                self.delimiter = delimiter
                self.quotechar = '"'
                self.doublequote = True
                self.skipinitialspace = True
                self.lineterminator = '\n'
                self.quoting = csv.QUOTE_MINIMAL
        
        # 尝试不同的分隔符
        separators_to_try = [sep, ',', '\t', ';', '|']  # 优先尝试检测出的分隔符
        
        for try_sep in separators_to_try:
            try:
                dialect = FlexibleDialect(try_sep)
                reader = csv.reader(io.StringIO(text_content), dialect=dialect)
                rows_data = []
                
                # 收集有效数据行
                for i, row in enumerate(reader):
                    if i > 1000:  # 限制读取行数防止内存问题
                        break
                    if row and any(cell.strip() for cell in row):  # 跳过空行
                        rows_data.append(row)
                
                if len(rows_data) >= 2:  # 至少需要标题行和一行数据
                    header_idx = max(0, start_row - 1)
                    if len(rows_data) > header_idx:
                        headers = rows_data[header_idx]
                        data_rows = rows_data[header_idx + 1:]
                        
                        # 检查列名是否合理（多个列且不包含分隔符）
                        if (len(headers) > 1 and 
                            not any(try_sep in str(h) for h in headers) and 
                            data_rows):
                            df = pd.DataFrame(data_rows, columns=headers)
                            return df
            except Exception:
                continue
        
        errors.append("CSV模块解析失败: 所有分隔符都无效")
    except Exception as e:
        errors.append(f"CSV模块解析失败: {str(e)}")
    
    # 策略4: 手动逐行解析（最后的尝试）
    try:
        text_content = content.decode(encoding, errors='replace')
        lines = [line.strip() for line in text_content.split('\n') if line.strip()]
        
        if len(lines) < 2:
            raise ValueError("文件至少需要包含标题行和一行数据")
        
        def smart_split(line, delimiter):
            """智能分割，处理引号内的分隔符"""
            if delimiter not in line:
                return [line.strip().strip('"')]
            
            parts = []
            current = ""
            in_quotes = False
            i = 0
            
            while i < len(line):
                char = line[i]
                
                if char == '"':
                    # 检查是否是转义的双引号
                    if in_quotes and i + 1 < len(line) and line[i + 1] == '"':
                        current += '"'
                        i += 1
                    else:
                        in_quotes = not in_quotes
                elif char == delimiter and not in_quotes:
                    # 在引号外遇到分隔符
                    cleaned = current.strip().strip('"').strip()
                    parts.append(cleaned)
                    current = ""
                else:
                    current += char
                i += 1
            
            # 添加最后一个字段
            cleaned = current.strip().strip('"').strip()
            parts.append(cleaned)
            
            # 过滤空字段（但保留在中间的空字段）
            if len(parts) > 1:
                # 只在有多个字段时才过滤，避免过度过滤
                return parts
            else:
                return [p for p in parts if p or len(parts) == 1]
        
        # 解析头部 - 尝试不同分隔符
        separators_to_try = [sep, ',', '\t', ';', '|']
        
        for try_sep in separators_to_try:
            try:
                header_line_idx = max(0, start_row - 1)
                if len(lines) <= header_line_idx:
                    continue
                    
                headers = smart_split(lines[header_line_idx], try_sep)
                headers = [h.strip() for h in headers if h.strip()]
                
                if len(headers) <= 1:  # 如果只有一列，则这个分隔符不正确
                    continue
                
                # 解析数据行
                data_rows = []
                for line in lines[header_line_idx + 1:]:
                    if line.strip():
                        cells = smart_split(line, try_sep)
                        
                        # 补齐或截断到标题列数
                        while len(cells) < len(headers):
                            cells.append("")
                        cells = cells[:len(headers)]
                        
                        data_rows.append(cells)
                
                if not data_rows:
                    continue
                
                # 检查解析质量：数据行的列数应该与标题一致
                valid_rows = 0
                for row in data_rows[:10]:  # 检查前10行
                    if len(row) == len(headers):
                        valid_rows += 1
                
                if valid_rows >= len(data_rows[:10]) * 0.8:  # 至少80%的行格式正确
                    df = pd.DataFrame(data_rows, columns=headers)
                    return df
                    
            except Exception:
                continue
                
        raise ValueError("所有手动解析策略都失败")
        
    except Exception as e:
        errors.append(f"手动解析失败: {str(e)}")
    
    # 所有策略都失败
    error_msg = "CSV解析失败，尝试了以下方法:\n" + "\n".join([f"{i+1}. {err}" for i, err in enumerate(errors)])
    raise HTTPException(400, error_msg)

@app.post("/api/upload")
async def upload(file: UploadFile = File(...), sheet: str | None = Form(None), start_row: int = Form(1)):
    name = file.filename or "upload"
    content = await file.read()
    ext = (name.split(".")[-1] or "").lower()
    
    if not content:
        raise HTTPException(400, "文件为空")
    
    try:
        if ext in ("csv", "tsv", "txt"):
            # 检测编码
            detected = chardet.detect(content)
            encoding = detected.get('encoding', 'utf-8') if detected else 'utf-8'
            
            # 如果检测置信度低，尝试常见编码
            if detected and detected.get('confidence', 0) < 0.7:
                for enc in ['utf-8', 'gbk', 'gb2312', 'latin-1', 'cp1252']:
                    try:
                        content.decode(enc)
                        encoding = enc
                        break
                    except:
                        continue
            
            # 智能检测分隔符
            sep = detect_separator(content, encoding)
            
            # 如果是特定文件类型，覆盖检测结果
            if ext == "tsv":
                sep = "\t"
            elif ext == "csv" and sep == '\t':  # CSV文件不应该是制表符分隔
                sep = ","
            
            # 使用强大的CSV解析函数
            df = parse_csv_robust(content, encoding, sep, start_row)
            
        else:
            # Excel文件处理
            sheet_arg = None
            if sheet:
                try:
                    sheet_arg = int(sheet)
                except:
                    sheet_arg = sheet
            
            try:
                df = pd.read_excel(
                    BytesIO(content),
                    sheet_name=sheet_arg,
                    header=start_row-1 if start_row > 1 else 0,
                    engine=None,
                    na_values=['', 'NULL', 'null', 'N/A', 'n/a', 'NA', 'na'],
                    keep_default_na=True
                )
                if isinstance(df, dict):
                    first_key = list(df.keys())[0]
                    df = df[first_key]
            except Exception as e:
                raise HTTPException(400, f"Excel文件读取失败: {str(e)}")
        
        # 数据清理和转换
        if df.empty:
            raise HTTPException(400, "文件不包含任何数据")
        
        # 清理列名
        df.columns = [str(col).strip() if col is not None else f'Column_{i}' 
                     for i, col in enumerate(df.columns)]
        
        # 删除完全空的行
        df = df.dropna(how='all')
        
        if df.empty:
            raise HTTPException(400, "清理后文件不包含任何数据")
        
        # 数据类型转换
        for col in df.columns:
            try:
                # 尝试转换为数值
                numeric_col = pd.to_numeric(df[col], errors='coerce')
                # 如果转换成功且不是全部NaN，则使用转换后的结果
                if not numeric_col.isna().all():
                    df[col] = numeric_col
            except:
                # 转换失败，保持原始数据
                pass
        
        # 处理特殊浮点数值
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(pd.notnull(df), None)
        
        # 转换为记录列表，确保JSON序列化安全
        rows = [{str(k): safe_convert_value(v) for k, v in rec.items()} 
                for rec in df.to_dict(orient="records")]
        
        # 获取字段类型信息
        fields = {}
        for col in df.columns:
            dtype = str(df[col].dtype)
            if dtype.startswith('int') or dtype.startswith('float'):
                fields[str(col)] = 'number'
            elif dtype == 'bool':
                fields[str(col)] = 'boolean'
            else:
                fields[str(col)] = 'string'
        
        if not rows:
            raise HTTPException(400, "文件中没有找到有效数据")
        
        return {
            "filename": name,
            "count": len(rows),
            "rows": rows,
            "fields": fields,
            "message": f"成功解析 {len(rows)} 行数据，共 {len(df.columns)} 列"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"文件 '{name}' 解析失败: {str(e)}")

@app.get("/api/prefs/list")
def prefs_list(): return {"items": prefs.list_all()}

@app.get("/api/prefs/get")
def prefs_get(collection:str, name:str): return {"doc": prefs.get(collection,name)}

@app.post("/api/prefs/save")
def prefs_save(body:dict): return {"ok": prefs.save(body)}

@app.delete("/api/prefs/delete")
def prefs_del(collection:str, name:str): return {"ok": prefs.delete(collection,name)}
