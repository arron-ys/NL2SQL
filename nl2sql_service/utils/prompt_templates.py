"""
Prompt Templates Module

定义所有 LLM 提示模板，用于各个阶段的自然语言处理。
基于详细设计文档附录A的定义。
"""
# ============================================================
# Stage 1: Sub-Query Decomposition (查询分解)
# ============================================================
PROMPT_SUBQUERY_DECOMPOSITION = """你是一个专业的查询分解助手（Query Decomposer）。你的任务是将用户的复杂查询问题拆解为多个**原子子查询**。

当前日期：{current_date}

用户问题：
{question}

请将上述问题拆解为多个原子子查询。每个子查询应该是独立的、可单独执行的查询。

**时间处理规则（必须严格遵守，违者视为错误输出）：**
- **[职责边界]** Stage1 **不承担时间口径决策**，也**不承担时间范围解析/补全**。时间意图的权威来源是用户原始问题（raw_question），不在 Stage1 的 `description` 中做任何“时间口径改写”。
- **[禁止行为 1] 严禁日期化模糊时间词**：以下“模糊时间词/短语”在 `description` 中**必须原样保留**，不得改写为任何具体日期/日期区间/具体天：
  - 中文：最近、目前、当前、现阶段、近期、这段时间、这阵子、近来、近日、近段时间、近一段时间、最近一段时间、最近这段时间、最近这阵子、近些天、最近几天、近几天
  - English: recent, recently, lately, these days, nowadays, currently, at present, in recent times, in the past period
  - 错误示例（禁止）：用户说“最近的销售额” → description 写成“2025-01-15 的销售额” ❌
  - 正确示例：用户说“最近的销售额” → description 仍写“最近的销售额” ✅
- **[禁止行为 2] 严禁新增时间范围**：如果用户问题中**没有任何时间意图**，则在 `description` 中**严禁**添加任何时间范围描述（不要加“近30天/今年/本月/最近”等）。
- **[禁止行为 3] 严禁改写任何时间表达**：即使用户提到“今年/本月/上周/昨天/2024年1月/最近30天”等，Stage1 也必须在 `description` 中**原样保留用户的时间表达**，不得转换为绝对日期、不得补齐起止边界、不得推断自然周/月/季度边界。
  - 示例：用户说“今年销售额” → description 写“今年销售额”，不要改成“2025年销售额”。
  - 示例：用户说“最近30天销售额” → description 写“最近30天销售额”，不要改成具体日期区间。
- **[目的说明]** Stage1 的 `description` 只用于**指标/维度/过滤意图**的清晰化与分解，时间口径由后续阶段处理。

**反幻觉规则：**
- 只基于用户问题中明确提到的信息进行分解，不要添加用户未提及的假设。
- 如果问题本身已经是原子查询，则只返回一个子查询。
- 如果问题包含多个独立的问题，则拆解为多个子查询。
- 每个子查询应包含完整的查询意图，不依赖其他子查询的结果。

请以 JSON 格式返回结果，格式如下：
{
  "sub_queries": [
    {
      "id": "sq_1",
      "description": "第一个子查询的自然语言描述（注意：不得改写或补全任何时间表达）"
    },
    {
      "id": "sq_2",
      "description": "第二个子查询的自然语言描述（注意：不得改写或补全任何时间表达）"
    }
  ]
}

**输出要求：**
- sub_queries 必须是一个数组
- 至少包含一个子查询
- 每个子查询必须包含 id 和 description 字段
- description 必须是清晰、完整的自然语言查询描述
- description 中的时间表达：只能来自用户原话，必须原样保留，不得新增、不得改写、不得补全
"""


# ============================================================
# Stage 2: Plan Generation (计划生成)
# ============================================================
PROMPT_PLAN_GENERATION = """
# Role
You are an expert Data Analyst AI. Your goal is to map the user's natural language question into a structured **JSON Query Plan**.

# Context
- Current Date: {current_date} (Format: YYYY-MM-DD)
- Raw Question: "{raw_question}"
- Sub-Query Description: "{sub_query_description}"

**CRITICAL - Time Range Authority (时间范围权威来源，必须严格遵守)：**
- Authority: `time_range` MUST be inferred primarily from **Raw Question**.
- Sub-Query Description is ONLY for metric/dimension/filter intent; it MUST NOT override time intent.
- You MUST NOT infer `time_range` solely from any date text that appears in Sub-Query Description while ignoring Raw Question.

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
     - **DETAIL Query Golden Rule**:
       - If the `intent` is `DETAIL`, you MUST select at least TWO dimensions:
         1. The dimension the user explicitly asked for.
         2. The primary identifier dimension of the entity.
       - **Reason:** This is to ensure each row in the result is uniquely identifiable by the user.

3. **Filters**:
   - Put ALL filter conditions here (do not distinguish between WHERE and HAVING).
   - `id`: Must be a valid Metric ID or Dimension ID.
   - `op`: Choose from ["EQ", "NEQ", "IN", "NOT_IN", "GT", "LT", "GTE", "LTE", "BETWEEN", "LIKE"].
   - `values`: Must be a list. **Strictly keep original types**. Do NOT convert string IDs (e.g., "007") to numbers (7).

4. **Time Range (必须严格遵守；输出必须可被 JSON 解析)：**
   - **Rule 0 - Precedence**:
     - Time range decisions MUST follow the rules below in order.
     - If multiple cues exist, choose the most explicit one.
   - **Rule 1 - ALL_TIME (only when explicitly asked)**:
     - If Raw Question explicitly indicates all history / no time limit:
       - Chinese: "全量历史", "不限时间", "全部历史", "所有时间"
       - English: "all time", "all history", "no time limit"
     - Output: `{{ "type": "ALL_TIME" }}`
     - MUST NOT use `null` to represent ALL_TIME.
   - **Rule 2 - Explicit relative window with number (MUST output LAST_N)**:
     - If Raw Question matches an explicit pattern like:
       - Chinese: "最近/近/过去/近N天/近N周/近N个月/过去N天/过去N周/过去N个月/近N日"
       - English: "last N days/weeks/months", "past N days/weeks/months"
     - Output: `{{ "type": "LAST_N", "value": N, "unit": "DAY" | "WEEK" | "MONTH" }}`
     - Examples:
       - "最近30天的销售额" -> `{{ "type": "LAST_N", "value": 30, "unit": "DAY" }}`
       - "last 7 days revenue" -> `{{ "type": "LAST_N", "value": 7, "unit": "DAY" }}`
   - **Rule 3 - Vague time cue (MUST output null; let Stage3 + YAML inject defaults)**:
     - If Raw Question contains a vague cue WITHOUT an explicit number window (Rule 2 not matched):
       - Chinese: 最近、目前、当前、现阶段、近期、这段时间、这阵子、近来、近日、近段时间、近一段时间、最近一段时间、最近这段时间、最近这阵子、近些天、最近几天、近几天
       - English: recent, recently, lately, these days, nowadays, currently, at present, in recent times, in the past period
     - Output: `null`
     - Examples:
       - "最近的销售额" -> `null`
       - "currently active customers" -> `null`
   - **Rule 4 - Explicit absolute time (output ABSOLUTE)**:
     - If Raw Question includes explicit dates or explicit date ranges that can be mapped to concrete boundaries, output:
       - `{{ "type": "ABSOLUTE", "start": "YYYY-MM-DD", "end": "YYYY-MM-DD" }}`
     - Requirements:
       - `start` and `end` MUST be top-level fields (no nested object).
       - Use ISO date format YYYY-MM-DD.
     - Examples:
       - "2023-01-01 到 2023-01-31 的销售额" -> `{{ "type": "ABSOLUTE", "start": "2023-01-01", "end": "2023-01-31" }}`
       - "2024年1月" -> map to an absolute range for that month.
   - **Rule 5 - No time intent (output null)**:
     - If Raw Question contains no time intent at all, output `null`.
   - **Rule 6 - Time intent exists but cannot be resolved (output null; NOTE behavior)**:
     - If Raw Question has time intent but boundaries cannot be determined (e.g., "之前", "前一阵", "某天/某月/某年" without context),
       output `null` (DO NOT guess).
     - IMPORTANT NOTE: This may trigger Stage3 to raise an ambiguity error for clarification if it detects a time cue that is not vague.

5. **Order By & Limit**:
   - `order_by`: Only sort by metrics or dimensions that are selected in the query.
   - `limit`: Set to integer if user specifies (e.g., "Top 10"). Otherwise `null`.

# JSON Output Format
Return ONLY a valid JSON object matching this structure.
**IMPORTANT:** Your output must be **standard JSON** (no comments, no trailing commas).

Canonical JSON example (structure only; replace IDs/values according to the user's question and the schema context):
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
  "time_range": null,
  "order_by": [
    {{ "id": "METRIC_ID", "direction": "DESC" }}
  ],
  "limit": 100
}}


Valid `time_range` fragments (choose EXACTLY ONE according to the Time Range rules above):
- null
- {{ "type": "LAST_N", "value": 30, "unit": "DAY" }}
- {{ "type": "ABSOLUTE", "start": "2023-01-01", "end": "2023-01-31" }}
- {{ "type": "ALL_TIME" }}
"""



# ============================================================
# Stage 6: Data Insight (数据洞察 - 成功路径)
# ============================================================
PROMPT_DATA_INSIGHT = """
# 角色
你是一个友好且严谨的数据助手。你的任务是仅基于下方提供的【查询结果数据】与【业务上下文】，生成简洁、清晰、口径完整的回答。

# 核心规则（必须遵守）
1. 【严禁杜撰】绝对禁止编造输入中不存在的任何信息、口径或数字。
2. 【忠于数据】所有结论与数值必须能从【查询结果数据】直接得到；不要解释原因，不要推断业务逻辑。
3. 【禁止泄露内部标识】回答中严禁出现任何内部 ID（例如 METRIC_*、DIM_* 等）。即使输入里出现，也不要复述。
4. 【口径来自上下文】时间范围与单位必须来自【业务上下文】；如果业务上下文不足，请明确说明“未提供时间范围/单位信息”，不要猜。

# 输入信息
- 用户原始问题: {raw_question}
- 业务上下文（可能为空或为占位提示）:
{context_summary}
- 查询结果数据（Markdown 格式）:
{query_result_data}
- 查询元数据:
  - 返回行数: {row_count}
  - 是否截断: {is_truncated}   # 注意：值为“是/否”
  - 执行耗时(毫秒): {execution_latency_ms}

# 输出要求（按顺序输出）
1. 【结论先行】第一句话直接回答用户问题；如业务上下文提供了时间范围与单位，请自然地写进这句话里。
2. 【数值表达要人性化】
   - 若单位为金额（上下文里单位包含“元/万元/亿元”的语义）：当数值 >= 1e8 用“X.XX 亿元”；>= 1e4 用“X.XX 万元”；否则用“X.XX 元”（保留两位小数）。
   - 若单位为计数（如“个/单/笔”）：输出整数 + 单位。
   - 若单位为百分比（“%”）：输出“X.XX%”。
   - 避免输出难读的长串数字；允许在括号中补充“精确值”（可选），但不得改变单位口径。
3. 【多行结果要汇总】如果结果多行，先概括整体，再列出最关键的 1-3 个要点（只引用数据中存在的列/维度）。
4. 【空结果与截断】数据为空要明确说明；若“是否截断”为“是”，末尾提醒“结果可能不完整”。

请输出一段自然语言答案，不要输出 JSON。
"""


# ============================================================
# Stage 6: Clarification (澄清问题 - 澄清路径)
# ============================================================
PROMPT_CLARIFICATION = """你是一个友好、专业的数据查询助手。当用户的查询存在歧义或信息不足时，你需要生成一个礼貌、清晰的澄清问题。

用户原始问题：
{raw_question}

无法确定的信息：
{uncertain_information}

请生成一个礼貌的澄清问题，帮助用户明确他们的查询意图。澄清问题应该：
1. 语气友好、专业
2. 明确指出需要澄清的具体点
3. 如果可能，提供一些选项供用户选择
4. 简洁明了，不要过于冗长

请以自然语言的形式返回澄清问题，不要使用JSON格式。
"""
