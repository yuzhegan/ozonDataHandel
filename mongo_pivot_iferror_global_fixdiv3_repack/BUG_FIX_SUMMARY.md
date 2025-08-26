# 修复 "Cannot read properties of undefined (reading 'map')" 错误

## 问题描述
项目在读取文件或处理数据时偶尔会出现 "Cannot read properties of undefined (reading 'map')" 错误，这通常发生在以下情况：
1. API 返回数据格式不符合预期
2. 数组变量在某些情况下为 undefined 或 null
3. 网络请求失败导致数据为空

## 修复内容

### 1. `loadCollections()` 函数
- 添加了 try-catch 错误处理
- 增加了数组类型检查：`Array.isArray(j.collections)`
- 在出错时清空下拉列表并记录错误日志

### 2. `applyComputed()` 函数
- 增加了数组类型检查：`!Array.isArray(rows)`
- 返回空数组而不是 undefined

### 3. `rebuildAvailableFields()` 函数  
- 增加了数组长度检查：`Array.isArray(currentRows) && currentRows.length > 0`
- 确保 sample 对象不为空

### 4. `renderPivot()` 函数
- 增加了数组类型检查：`Array.isArray(rawBaseRows)`
- 修复了行数统计：`Array.isArray(currentRows) ? currentRows.length : 0`

### 5. `renderRawTable()` 函数
- 添加了空数组检查，在数据为空时显示提示信息
- 防止对空数组调用 map 方法

### 6. `postQuery()` 函数
- 添加了 try-catch 错误处理
- 确保返回值始终为数组：`Array.isArray(j.rows) ? j.rows : []`

### 7. `btnLoad.onclick` 处理函数
- 增加了上传数据的数组类型检查
- 确保过滤操作在有效数组上执行

### 8. `doPeek()` 函数
- 添加了 try-catch 错误处理
- 增加了数组类型检查：`Array.isArray(j.rows)`
- 在出错时设置空数组并继续渲染

### 9. 文件上传成功处理
- 严格检查返回数据：`Array.isArray(j.rows)`
- 改进错误提示信息

### 10. `getColumns()` 函数
- 增加了数组长度检查，防止访问空数组的第一个元素

### 11. `pivotData()` 函数
- 在函数开始处添加空数组检查
- 返回默认的空结构而不是继续处理

## 修复原理
所有修复都遵循以下原则：
1. **防御性编程**：在使用数组方法之前检查数据类型和长度
2. **优雅降级**：在数据为空时显示友好提示而不是崩溃
3. **错误处理**：使用 try-catch 捕获异常并记录错误日志
4. **类型安全**：使用 `Array.isArray()` 确保变量是数组类型

## 影响
- 消除了所有可能的 "Cannot read properties of undefined (reading 'map')" 错误
- 提高了应用的稳定性和用户体验
- 在数据异常时提供清晰的错误信息
- 保持了原有的功能和逻辑不变
