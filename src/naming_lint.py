from __future__ import annotations

import re
from typing import Any


SLOT_NAMING_GUARDRAILS = """## 命名硬约束（必须遵守）

- `name` 必须是业务语义英文名，使用 snake_case；禁止把中文逐字转成拼音来交差。
- 若字段语义与 `base_slots` 已有项接近，必须直接复用 base 的 `name`，不要新造近义 extended。
- 优先复用通用 base：
  - 行政区划 / 省市区县 / 区划代码 -> `region_code`
- 反例 / 正例：
  - ❌ 户籍地派出所编码 -> `hu_ji_di_pai_chu_suo_bian_ma`
    ✅ `household_police_station_code`
  - ❌ 市局代码 -> `shi_ju_dai_ma`
    ✅ `city_bureau_code`
  - ❌ 省市区县 -> `sheng_shi_xian_qu`
    ✅ `region_code`
- 名称尽量控制在 25 个字符以内；如果语义英文名略长，也优先保证语义正确，不要退化成拼音。"""


_SNAKE_CASE_RE = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$")
_TOKEN_RE = re.compile(r"[a-z]+")

# 常见英文短词 / 缩写，避免和拼音音节冲突造成误报。
_PINYIN_EXCLUSION_TOKENS = {
    "ai", "am", "an", "api", "app", "as", "at", "be", "by",
    "do", "go", "he", "id", "if", "in", "ip", "is", "it",
    "lat", "log", "lon", "lng", "mac", "max", "me", "min",
    "no", "of", "on", "or", "so", "sql", "to", "up", "url",
    "us", "vin", "vt", "we",
}

# 常用无声调拼音音节表，用于识别“逐字拼音直译”的 slot name。
_PINYIN_SYLLABLES = set("""
a ai an ang ao
ba bai ban bang bao bei ben beng bi bian biao bie bin bing bo bu
ca cai can cang cao ce cen ceng cha chai chan chang chao che chen cheng chi chong chou chu chua chuai chuan chuang chui chun chuo ci cong cou cu cuan cui cun cuo
da dai dan dang dao de dei deng di dia dian diao die ding diu dong dou du duan dui dun duo
e ei en eng er
fa fan fang fei fen feng fo fou fu
ga gai gan gang gao ge gei gen geng gong gou gu gua guai guan guang gui gun guo
ha hai han hang hao he hei hen heng hong hou hu hua huai huan huang hui hun huo
ji jia jian jiang jiao jie jin jing jiong jiu ju juan jue jun
ka kai kan kang kao ke kei ken keng kong kou ku kua kuai kuan kuang kui kun kuo
la lai lan lang lao le lei leng li lia lian liang liao lie lin ling liu lo long lou lu luan lun luo lv lve
ma mai man mang mao me mei men meng mi mian miao mie min ming miu mo mou mu
na nai nan nang nao ne nei nen neng ni nian niang niao nie nin ning niu nong nou nu nuan nue nuo nv nve
o ou
pa pai pan pang pao pei pen peng pi pian piao pie pin ping po pou pu
qi qia qian qiang qiao qie qin qing qiong qiu qu quan que qun
ran rang rao re ren reng ri rong rou ru rua ruan rui run ruo
sa sai san sang sao se sen seng sha shai shan shang shao she shei shen sheng shi shou shu shua shuai shuan shuang shui shun shuo si song sou su suan sui sun suo
ta tai tan tang tao te teng ti tian tiao tie ting tong tou tu tuan tui tun tuo
wa wai wan wang wei wen weng wo wu
xi xia xian xiang xiao xie xin xing xiong xiu xu xuan xue xun
ya yan yang yao ye yi yin ying yo yong you yu yuan yue yun
za zai zan zang zao ze zei zen zeng zha zhai zhan zhang zhao zhe zhen zheng zhi zhong zhou zhu zhua zhuai zhuan zhuang zhui zhun zhuo zi zong zou zu zuan zui zun zuo
""".split())


def tokenize_name(name: str) -> list[str]:
    return _TOKEN_RE.findall((name or "").lower())


def analyze_pinyin_tokens(name: str, threshold: float = 0.6) -> dict[str, Any]:
    tokens = tokenize_name(name)
    pinyin_tokens = [
        t for t in tokens
        if t in _PINYIN_SYLLABLES and t not in _PINYIN_EXCLUSION_TOKENS
    ]
    ratio = (len(pinyin_tokens) / len(tokens)) if tokens else 0.0
    return {
        "tokens": tokens,
        "pinyin_tokens": pinyin_tokens,
        "token_count": len(tokens),
        "pinyin_count": len(pinyin_tokens),
        "pinyin_ratio": ratio,
        "is_mostly_pinyin": bool(tokens) and len(tokens) >= 2 and ratio >= threshold,
    }


def validate_slot_name(
    name: str,
    *,
    source: str = "extended",
    base_slot_names: set[str] | None = None,
) -> list[str]:
    issues: list[str] = []
    clean_name = (name or "").strip()
    source = (source or "extended").strip().lower()

    if not clean_name:
        return ["缺少 name"]

    if not _SNAKE_CASE_RE.fullmatch(clean_name):
        issues.append(f"name `{clean_name}` 不是合法的 snake_case")

    if source == "base":
        if base_slot_names is not None and clean_name not in base_slot_names:
            issues.append(f"name `{clean_name}` 标为 base，但不在 base_slots 词表中")
        return issues

    analysis = analyze_pinyin_tokens(clean_name)
    if analysis["is_mostly_pinyin"]:
        tokens_text = ", ".join(analysis["pinyin_tokens"][:8])
        issues.append(
            f"name `{clean_name}` 疑似中文逐字拼音直译"
            f"（{analysis['pinyin_count']}/{analysis['token_count']} 个 token 命中拼音音节：{tokens_text}）"
        )
    return issues


def resolve_slot_source(slot: dict[str, Any], preferred_key: str = "from") -> str:
    for key in (preferred_key, "source", "from"):
        value = str(slot.get(key) or "").strip().lower()
        if value:
            return value
    return "extended"


def collect_slot_name_issues(
    slots: list[dict[str, Any]] | None,
    *,
    source_key: str = "from",
    base_slot_names: set[str] | None = None,
) -> list[str]:
    issues: list[str] = []
    for idx, slot in enumerate(slots or []):
        if not isinstance(slot, dict):
            continue
        name = str(slot.get("name") or "").strip()
        source = resolve_slot_source(slot, preferred_key=source_key)
        for issue in validate_slot_name(name, source=source, base_slot_names=base_slot_names):
            issues.append(f"slot[{idx}] {issue}")
    return issues


def format_naming_retry_feedback(issues: list[str]) -> str:
    lines = [
        "上一轮返回的 slot 命名不合格，请修正后重新返回完整 JSON。",
        "以下问题必须全部消除：",
    ]
    lines.extend(f"- {issue}" for issue in issues[:10])
    lines.extend([
        "- 只能使用业务语义英文名，禁止逐字拼音直译。",
        "- 若语义接近已有 base_slots，必须直接复用 base 的 name。",
        "- 重新输出完整 JSON，不要附加解释。",
    ])
    return "\n".join(lines)
