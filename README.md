# NL2SQL-Pro：可治理的“自然语言 → 指标口径 → SQL → 可审计结果”服务

> **把“问一句话”变成“可复现、可审计、可控权限”的分析查询。**  
> 核心思想：用 **强约束的 PLAN 数据契约** + **YAML 语义层（指标/维度/权限的单一事实源）**，把 NL2SQL 从“玄学生成 SQL”拉回到“工程化可控系统”。

---

## 这是什么

NL2SQL-Pro 是一个基于 FastAPI 的后端服务，提供端到端流水线：

**NL → subqueries → PLAN → SQL → Result → Answer**

它不是“直接让 LLM 写 SQL”，而是把 LLM 的职责限制在**生成结构化意图（PLAN Skeleton）**，并在服务端通过语义层与规则引擎进行**校验、补全、安全注入**，最终生成可执行 SQL 并返回结构化结果与自然语言解释。README 本身就是你仓库的门面：让访问者知道“为什么有用、能做什么、怎么用”。:contentReference[oaicite:0]{index=0}

---

## 解决什么问题（行业痛点）

传统 NL2SQL 常见问题是“看起来能跑、实际上不可用”：

- **口径不一致**：同一句“销售额”，不同人/不同系统算出来不同数字（指标定义散落在代码、BI、文档里）。
- **不可控/不可审计**：SQL 由模型即兴生成，出了错难定位；也很难做回归测试与质量评估。
- **权限与安全缺位**：谁能查什么、能不能下钻明细、如何做行级权限（RLS）往往靠应用层“补丁”。
- **工程不可维护**：模型换了/表结构变了/业务术语变了，就全线漂移。

---

## 我怎么解决（核心设计）

### 1) 强数据契约：PLAN 是系统内部的“核心协议”
PLAN 是整个链路的**中间态表达**与**标准输入/输出契约**：Stage2 产出 Skeleton Plan，Stage3 校验补全，Stage4 生成 SQL，Stage6 用于解释结果。  
关键约束：

- **扁平化**：不支持嵌套 Plan（复杂问题必须上游拆解）。
- **声明式**：只说“要什么”，不说“怎么 join / 怎么查”。
- **强语义依赖**：PLAN **只允许出现语义 ID**（如 `METRIC_GMV`、`DIM_CITY`、`LF_*`），严禁物理表名/字段名/幻觉字段。

> 这一步把“生成 SQL”拆成“生成可验证的意图（PLAN）”+“确定性编译（PLAN→SQL）”，**可测试、可审计、可持续演进**。

---

### 2) YAML 语义层：系统的“静态大脑”（Single Source of Truth）
语义层 YAML 是连接业务逻辑与技术实现的桥梁：  
- 对 LLM：是 RAG 检索的知识库（召回指标/维度/枚举/别名）  
- 对后端：是规则引擎（合法性校验、默认值补全、SQL 拼接、安全策略注入）

设计铁律（工程价值）：
- **高内聚**：定义/别名/枚举就地管理（避免散落）。
- **RAG-Native**：每个对象节点自带完整上下文，天然适配向量索引 Chunking。
- **单一事实来源**：指标口径/默认时间/强制过滤/权限矩阵只在 YAML 定义，代码只“读配置并执行”。

这类“语义层治理”的价值在现代数据体系里已经成为共识：目标是让指标成为“可复用、可治理的单一事实源”。:contentReference[oaicite:1]{index=1}

---

### 3) 安全与权限：从“事后校验”升级为“全链路执法”
权限不是“查完再过滤”，而是贯穿链路：

- **检索阶段（RAG Gate）**：按 Role 权限白名单过滤掉不可访问指标/维度（先天减少越权生成）。
- **SQL 生成阶段（Enforcement）**：强制注入 RLS 策略片段到 WHERE，实现行级隔离/最小权限。

语义安全配置示例（真实 YAML）：
- 角色：`ROLE_CEO / ROLE_SALES_STAFF / ROLE_HR_HEAD ...`
- 行级范围：`SELF / DEPT / COMPANY / REGION`
- RLS 片段：如销售仅看本人订单 `sales_rep_employee_number = {{ current_user.employee_id }}`

> 结果是：你能清楚回答“**谁**在**什么角色**下，能看**哪些指标/维度**，能不能下钻明细，SQL 到底注入了什么约束”。

---

## 端到端架构（你一眼就能看懂的流水线）

flowchart TD
    U[User NL Question] --> A[Stage 0/Request & Context]
    A --> B[Stage 1/Subquery Decomposition]
    B --> C[Stage 2/NL → PLAN Skeleton (LLM + RAG)]
    C --> D[Stage 3/PLAN Validate & Normalize]
    D --> E[Stage 4/PLAN → SQL Compiler<br/>(Semantic YAML + Security Injection)]
    E --> F[Stage 5/Execute SQL<br/>(MySQL/Postgres, Read-only)]
    F --> G[Stage 6/Result → Answer (LLM)]

    subgraph S[Semantic Brain (YAML Registry)]
        direction LR
        Y1[semantic_core.yaml]
        Y2[semantic_metrics.yaml]
        Y3[semantic_security.yaml]
        Y4[semantic_common.yaml]
    end

    %% 使用点状线(--.)表示“查询/参考”关系，与主流程的实线区分
    C --. retrieve/context .--> S
    D --. validate .--> S
    E --. compile/enforce .--> S




## 关键亮点（Key Features）

- **“意图-编译”分离**：LLM 只产出 **PLAN Skeleton**，后端用规则与语义层做**确定性编译** → 可控、可测、可回归。
- **强契约 PLAN**：全链路围绕统一数据结构流转 → 调试与阶段定位天然清晰（错在拆解 / Plan / 校验 / SQL / 执行 / 解释，一眼就能断层）。
- **语义层治理（YAML SSoT）**：指标 / 维度 / 枚举 / 默认时间 / 强制过滤统一落在配置 → 业务变更只改配置，不改代码。
- **默认值与兜底策略**：如指标自带 `default_time`（字段 + 时间窗口 + fallback），Plan 不完整也能自动补齐到“可执行形态”。
- **企业级权限模型**：角色 → 范围 → RLS 片段的显式配置与强制注入 → 越权不可生成、越权不可执行。
- **MVP 能力边界明确（可运营）**：明确列出 V1 不支持项（跨事实表、嵌套查询、自定义时间偏移等），并要求代码**熔断报错** → 避免“看似支持、实际乱跑”。

---

## 语义层长什么样（真实配置片段）

### Metric（指标）

- `METRIC_GMV`：`SUM(line_gmv)`，默认时间窗口近 30 天，默认过滤 `LF_REVENUE_VALID_ORDER`
- `METRIC_AOV`：比率指标（`METRIC_GMV / METRIC_ORDER_CNT`），支持 `safe_division`

示例（片段示意）：

```yaml
metrics:
  - metric_id: METRIC_GMV
    expr: "SUM(line_gmv)"
    default_time:
      time_field_id: TF_ORDER_DATE
      window: TW_LAST_30_DAYS
      fallback: TW_THIS_YEAR
    default_filters:
      - LF_REVENUE_VALID_ORDER

  - metric_id: METRIC_AOV
    expr: "safe_division(${METRIC_GMV}, ${METRIC_ORDER_CNT})"
```


## Entity（实体）与 Semantic View（语义视图）

- `ENT_SALES_ORDER_ITEM` → `semantic_view: v_sales_order_item`
- `ENT_EMPLOYEE` → `semantic_view: v_employee_profile`

> **MVP 阶段的关键设计**：SQL `FROM` 只指向语义视图（宽表），不动态推导复杂 `JOIN`，把复杂性固定在数据建模层（更稳、更可控）。

---

## 产品级的“可解释性”（不是瞎跑）

这个项目最像“工程系统”的地方：它天然支持把每一步都讲清楚——

- **Plan 是可读的**：你能看到模型到底选了什么指标、什么维度、什么过滤、什么时间范围。
- **校验是可解释的**：ID 是否存在、是否允许维度、枚举值是否落在集合内、是否触发不支持特性熔断。
- **SQL 是可审计的**：RLS 注入了什么、默认过滤加了什么、时间窗口落成了什么日期范围。

---

## 接口形态（服务端对外）

作为后端服务，它可以提供：

- `POST /nl2sql/plan`：只产出 PLAN（用于 Debug / 可视化 / 回归）
- `POST /nl2sql/sql`：产出 SQL（用于审计）
- `POST /nl2sql/execute`：执行并返回结构化结果 + Answer（端到端）

