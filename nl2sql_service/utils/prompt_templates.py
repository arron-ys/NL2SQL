"""
Prompt Templates Module

定义所有 LLM 提示模板，用于各个阶段的自然语言处理。
基于详细设计文档附录A的定义。
"""
# ============================================================
# Stage 1: Sub-Query Decomposition (子查询分解)
# ============================================================
PROMPT_SUBQUERY_DECOMPOSITION = """你是一个专业的查询分解助手（Query Decomposer）。你的任务是将用户的复杂查询问题拆解为多个原子子查询。

当前日期：{current_date}

用户问题：
{question}

请将上述问题拆解为多个原子子查询。每个子查询应该是独立的、可单独执行的查询。

**时间解析规则：**
- 当用户使用相对时间表达（如"今年"、"本月"、"上周"等）时，必须根据当前日期 {current_date} 转换为具体的绝对时间。
- 例如：如果当前日期是 2025-01-15，则"今年"应理解为"2025年"，"本月"应理解为"2025年1月"。
- 在子查询描述中，应使用明确的日期范围，避免使用模糊的相对时间表达。

**反幻觉规则：**
- 只基于用户问题中明确提到的信息进行分解，不要添加用户未提及的假设。
- 如果问题本身已经是原子查询，则只返回一个子查询。
- 如果问题包含多个独立的问题，则拆解为多个子查询。
- 每个子查询应该包含完整的查询意图，不依赖其他子查询的结果。

请以 JSON 格式返回结果，格式如下：
{{
  "sub_queries": [
    {{
      "id": "sq_1",
      "description": "第一个子查询的自然语言描述（已解析相对时间为绝对时间）"
    }},
    {{
      "id": "sq_2",
      "description": "第二个子查询的自然语言描述（已解析相对时间为绝对时间）"
    }}
  ]
}}

**输出要求：**
- sub_queries 必须是一个数组
- 至少包含一个子查询
- 每个子查询必须包含 id 和 description 字段
- description 应该是清晰、完整的自然语言查询描述，已将所有相对时间转换为绝对时间
"""


# ============================================================
# Stage 2: Plan Generation (计划生成)
# ============================================================
PROMPT_PLAN_GENERATION = """
# Role
You are an expert Data Analyst AI. Your goal is to map the user's natural language question into a structured **JSON Query Plan**.

# Context
- Current Date: {current_date} (Format: YYYY-MM-DD)
- User Question: "{user_query}"

# Available Schema (Retrieved Context)
You can ONLY use the Metrics and Dimensions listed below.
**CRITICAL RULE:** Do NOT invent new IDs. Do NOT use IDs that are not in this list.
---------------------------------------------------
{schema_context}
---------------------------------------------------

# Logic Rules & Constraints

1. **Intent Classification**:
   - `AGG`: If the user asks for aggregated numbers (e.g., "Total Sales", "Count orders").
   - `TREND`: If the user asks for trends over time (e.g., "Monthly sales trend", "Daily active users").
   - `DETAIL`: If the user asks for raw records (e.g., "List recent orders", "Show employee details").

2. **Metrics & Dimensions**:
   - Map user phrases to the exact `ID` from the Available Schema.
   - **Metrics**:
     - If user asks for comparison (e.g., "YoY", "同比"), set `compare_mode`="YOY".
     - If "MoM"/"环比", set `compare_mode`="MOM".
     - If "WoW"/"周环比", set `compare_mode`="WOW".
     - Otherwise, set `compare_mode`=null.
   - **Dimensions**:
     - **CRITICAL**: Only set `time_grain` (e.g., "DAY", "MONTH") if the dimension is marked with `| Is_Time: True` in the **Schema Context**.
     - For all other dimensions, `time_grain` MUST be null.

3. **Filters**:
   - Put ALL filter conditions here (do not distinguish between WHERE and HAVING).
   - `id`: Must be a valid Metric ID or Dimension ID.
   - `op`: Choose from ["EQ", "NEQ", "IN", "NOT_IN", "GT", "LT", "GTE", "LTE", "BETWEEN", "LIKE"].
   - `values`: Must be a list. **Strictly keep original types**. Do NOT convert string IDs (e.g., "007") to numbers (7).

4. **Time Range**:
   - **Relative**: If user says "last 7 days", use `{{ "type": "LAST_N", "value": 7, "unit": "DAY" }}`.
   - **Absolute**: If user says "2023-01-01 to 2023-01-31", use `{{ "type": "ABSOLUTE", "start": "2023-01-01", "end": "2023-01-31" }}`.
     **CRITICAL**: For ABSOLUTE type, `start` and `end` must be top-level fields. Do NOT nest them inside a `value` object.
   - **Missing**: If user mentions NO time, set `time_range` to `null`. (Do NOT guess a default time).
   
5. **Order By & Limit**:
   - `order_by`: Only sort by metrics or dimensions that are selected in the query.
   - `limit`: Set to integer if user specifies (e.g., "Top 10"). Otherwise `null`.

# JSON Output Format
Return ONLY a valid JSON object matching this structure.
**IMPORTANT:** The example below uses `//` comments to show different options. Your output must be **standard JSON** (no comments).

```json
{{
  "intent": "AGG",
  "metrics": [
    {{ "id": "METRIC_ID", "compare_mode": "YOY" }}
  ],
  "dimensions": [
    {{ "id": "DIM_ID", "time_grain": "MONTH" }}
  ],
  "filters": [
    {{ "id": "DIM_ID", "op": "EQ", "values": ["Value"] }}
  ],
  "time_range": {{
    // OPTION 1: Relative Time (Use this structure for "last N days")
    "type": "LAST_N",
    "value": 30,
    "unit": "DAY"
    
    // OPTION 2: Absolute Time (Use this structure for specific dates)
    // "type": "ABSOLUTE",
    // "start": "2023-01-01",
    // "end": "2023-01-31"
  }},
  "order_by": [
    {{ "id": "METRIC_ID", "direction": "DESC" }}
  ],
  "limit": 100
}}
"""


# ============================================================
# Stage 6: Data Insight (数据洞察 - 成功路径)
# ============================================================
PROMPT_DATA_INSIGHT = """你是一个专业的数据分析师。你的任务是根据SQL查询结果，生成清晰、准确的数据洞察总结。

用户原始问题：
{original_question}

查询结果数据：
{query_result_data}

查询结果元数据：
- 执行耗时：{execution_latency_ms} 毫秒
- 返回行数：{row_count} 行
- 是否截断：{is_truncated}

请基于上述查询结果，生成一个简洁、专业的数据洞察总结。总结应该：
1. 直接回答用户的问题
2. 突出关键数据和趋势
3. 使用清晰、易懂的语言
4. 如果数据为空或异常，应明确说明
5. 如果结果被截断，应在总结中提及

请以自然语言的形式返回总结，不要使用JSON格式。
"""


# ============================================================
# Stage 6: Clarification (澄清问题 - 澄清路径)
# ============================================================
PROMPT_CLARIFICATION = """你是一个友好、专业的数据查询助手。当用户的查询存在歧义或信息不足时，你需要生成一个礼貌、清晰的澄清问题。

用户原始问题：
{original_question}

无法确定的信息：
{uncertain_information}

请生成一个礼貌的澄清问题，帮助用户明确他们的查询意图。澄清问题应该：
1. 语气友好、专业
2. 明确指出需要澄清的具体点
3. 如果可能，提供一些选项供用户选择
4. 简洁明了，不要过于冗长

请以自然语言的形式返回澄清问题，不要使用JSON格式。
"""
