import asyncio
import itertools
import os
import random
import re
import string
import time
from typing import Iterator, Optional

import aiohttp
from colorama import Fore, Style, init

init(autoreset=True)

# ================= CẤU HÌNH =================
API_URL = "https://discord.com/api/v9/unique-username/username-attempt-unauthed"

# Dán link Webhook Discord để báo khi tìm thấy username trống (để trống nếu không dùng)
WEBHOOK_URL = ""

PROXY_FILE = "proxies.txt"       # Không có file hoặc file trống -> tự chạy NO-PROXY
AVAILABLE_FILE = "available.txt" # File lưu username còn trống
CHECKED_FILE = "checked.txt"     # Lưu lịch sử đã check để không check trùng

NO_PROXY_DELAY = 1.5             # Delay mặc định (giây) ở chế độ No-Proxy
PROXY_CONCURRENCY = 20           # Số luồng khi có proxy
REQUEST_TIMEOUT = 10
# ============================================

VOWELS = "aeiou"
CONSONANTS = "bcdfghjklmnprstvwxyz"  # Bỏ 'q' để tránh cụm khó đọc như 'qa', 'qo'
SOFT_ENDINGS = ("nd", "nt", "st", "rk", "lt", "sk", "ck", "ll", "ss", "nn", "th", "ch", "sh")
DIGITS = string.digits
SYMBOLS = "_."

LETTERS = string.ascii_lowercase
ALPHANUM = string.ascii_lowercase + string.digits
ALL_CHARS = string.ascii_lowercase + string.digits + "_."


def is_valid_username(name: str) -> bool:
    return 2 <= len(name) <= 32 and ".." not in name


def format_proxy(raw: str) -> Optional[str]:
    raw = raw.strip()
    if not raw or raw.startswith("#"):
        return None
    if raw.startswith(("http://", "https://")):
        return raw
    parts = raw.split(":")
    if len(parts) == 4 and "@" not in raw:
        ip, port, user, pwd = parts
        return f"http://{user}:{pwd}@{ip}:{port}"
    return f"http://{raw}"


class ProxyPool:
    def __init__(self, path: str):
        self.proxies: list[str] = []
        self.cooldowns: dict[str, float] = {}
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    p = format_proxy(line)
                    if p:
                        self.proxies.append(p)
        self._cycle = itertools.cycle(self.proxies) if self.proxies else None

    @property
    def enabled(self) -> bool:
        return len(self.proxies) > 0

    def get(self) -> Optional[str]:
        if not self._cycle:
            return None
        now = time.monotonic()
        for _ in range(len(self.proxies)):
            proxy = next(self._cycle)
            if self.cooldowns.get(proxy, 0) <= now:
                return proxy
        return None

    def cooldown(self, proxy: Optional[str], seconds: float):
        if proxy:
            self.cooldowns[proxy] = time.monotonic() + seconds


def load_checked() -> set[str]:
    if not os.path.exists(CHECKED_FILE):
        return set()
    with open(CHECKED_FILE, "r", encoding="utf-8") as f:
        return {line.strip().lower() for line in f if line.strip()}


# ================= BỘ SINH USERNAME =================
def make_pronounceable_4l() -> str:
    """Sinh tên 4 chữ cái dễ đọc kiểu kamo, evan, kain, volt..."""
    pattern = random.choices(
        ["CVCV", "VCVC", "CVVC", "CV_END"],
        weights=[45, 25, 15, 15],
        k=1,
    )[0]
    c = lambda: random.choice(CONSONANTS)
    v = lambda: random.choice(VOWELS)

    if pattern == "CVCV":
        return f"{c()}{v()}{c()}{v()}"
    if pattern == "VCVC":
        return f"{v()}{c()}{v()}{c()}"
    if pattern == "CVVC":
        return f"{c()}{v()}{v()}{c()}"
    return f"{c()}{v()}{random.choice(SOFT_ENDINGS)}"


def make_pronounceable_4c() -> str:
    """Sinh tên 4 ký tự dễ đọc: gốc 3 chữ phát âm được + 1 số hoặc _ ."""
    c = lambda: random.choice(CONSONANTS)
    v = lambda: random.choice(VOWELS)
    d = lambda: random.choice(DIGITS)
    s = lambda: random.choice(SYMBOLS)

    pattern = random.choices(
        ["CVC_D", "CVC_S", "S_CVC", "CV_S_C", "C_S_VC", "C_LEET_CV"],
        weights=[30, 20, 15, 15, 10, 10],
        k=1,
    )[0]

    if pattern == "CVC_D":
        return f"{c()}{v()}{c()}{d()}"       # vd: kex7, vok2
    if pattern == "CVC_S":
        return f"{c()}{v()}{c()}{s()}"       # vd: zex., kiv_
    if pattern == "S_CVC":
        return f"{s()}{c()}{v()}{c()}"       # vd: .kiv, _zex
    if pattern == "CV_S_C":
        return f"{c()}{v()}{s()}{c()}"       # vd: ki.v, ka_z
    if pattern == "C_S_VC":
        return f"{c()}{s()}{v()}{c()}"       # vd: k.iv, z_ex
    # Kiểu số thay nguyên âm dễ đọc (0, 3, 4, 1)
    leet = random.choice("0134")
    return f"{c()}{leet}{c()}{v()}"          # vd: z0ra, m4ki, k3vo


def single_mode_generator(mode_key: str, seen: set[str]) -> Iterator[tuple[str, str]]:
    """Trả về (username, nhãn_chế_độ) không trùng lặp."""
    spec = MODES[mode_key]
    label = spec["label"]
    kind = spec["kind"]

    if kind == "exhaustive":
        length = spec["length"]
        alphabet = spec["alphabet"]
        pool = [
            "".join(chars)
            for chars in itertools.product(alphabet, repeat=length)
            if is_valid_username("".join(chars))
        ]
        random.shuffle(pool)
        for name in pool:
            if name not in seen:
                seen.add(name)
                yield name, label

    elif kind == "random":
        length = spec["length"]
        alphabet = spec["alphabet"]
        retries = 0
        while retries < 5000:
            name = "".join(random.choices(alphabet, k=length))
            if is_valid_username(name) and name not in seen:
                seen.add(name)
                retries = 0
                yield name, label
            else:
                retries += 1

    elif kind == "pronounceable_4l":
        retries = 0
        while retries < 5000:
            name = make_pronounceable_4l()
            if is_valid_username(name) and name not in seen:
                seen.add(name)
                retries = 0
                yield name, label
            else:
                retries += 1

    elif kind == "pronounceable_4c":
        retries = 0
        while retries < 5000:
            name = make_pronounceable_4c()
            if is_valid_username(name) and name not in seen:
                seen.add(name)
                retries = 0
                yield name, label
            else:
                retries += 1


def multi_mode_generator(mode_keys: list[str], seen: set[str]) -> Iterator[tuple[str, str]]:
    """Trộn xen kẽ các chế độ được chọn; chế độ nào quét cạn sẽ tự loại bỏ."""
    active_gens = [single_mode_generator(k, seen) for k in mode_keys]
    while active_gens:
        idx = random.randrange(len(active_gens))
        try:
            yield next(active_gens[idx])
        except StopIteration:
            active_gens.pop(idx)


MODES = {
    "1": {"label": "2l",        "desc": "2 chữ cái (vd: ax, kp)",                    "kind": "exhaustive", "length": 2, "alphabet": LETTERS},
    "2": {"label": "2c",        "desc": "2 ký tự: chữ + số + _ .",                   "kind": "exhaustive", "length": 2, "alphabet": ALL_CHARS},
    "3": {"label": "3l",        "desc": "3 chữ cái (vd: kax, miv)",                  "kind": "exhaustive", "length": 3, "alphabet": LETTERS},
    "4": {"label": "3c",        "desc": "3 ký tự: chữ + số + _ .",                   "kind": "exhaustive", "length": 3, "alphabet": ALL_CHARS},
    "5": {"label": "4l",        "desc": "4 chữ cái ngẫu nhiên",                      "kind": "random",     "length": 4, "alphabet": LETTERS},
    "6": {"label": "4c",        "desc": "4 ký tự: chữ + số + _ .",                   "kind": "random",     "length": 4, "alphabet": ALL_CHARS},
    "7": {"label": "4c-an",     "desc": "4 ký tự sạch: chỉ chữ + số (không _ .)",    "kind": "random",     "length": 4, "alphabet": ALPHANUM},
    "8": {"label": "4l-dep",    "desc": "4l dễ đọc (xen kẽ phụ âm - nguyên âm)",     "kind": "pronounceable_4l"},
    "9": {"label": "4c-dep",    "desc": "4c dễ đọc (gốc 3 chữ đẹp + số / _ .)",      "kind": "pronounceable_4c"},
}


# ================= CHECKER CORE =================
class Stats:
    def __init__(self):
        self.checked = 0
        self.available = 0
        self.taken = 0
        self.rate_limits = 0
        self.delay = NO_PROXY_DELAY
        self.lock = asyncio.Lock()


async def send_webhook(session: aiohttp.ClientSession, username: str, tag: str):
    if not WEBHOOK_URL.strip():
        return
    try:
        await session.post(
            WEBHOOK_URL.strip(),
            json={"content": f"✅ **Username Discord trống (`{tag}`):** `{username}`"},
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
        )
    except Exception:
        pass


def append_line(filepath: str, text: str):
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(text + "\n")


async def check_username(
    session: aiohttp.ClientSession,
    username: str,
    tag: str,
    proxy_pool: ProxyPool,
    stats: Stats,
):
    headers = {
        "Content-Type": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
    }

    while True:
        proxy = proxy_pool.get() if proxy_pool.enabled else None
        if proxy_pool.enabled and proxy is None:
            await asyncio.sleep(1.5)
            continue

        try:
            async with session.post(
                API_URL,
                json={"username": username},
                headers=headers,
                proxy=proxy,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    taken = data.get("taken", True)

                    async with stats.lock:
                        stats.checked += 1
                        append_line(CHECKED_FILE, username)

                        if not taken:
                            stats.available += 1
                            append_line(AVAILABLE_FILE, username)
                            print(
                                f"{Fore.GREEN}[AVAILABLE] [{tag:<6}] {username:<8} "
                                f"(Checked: {stats.checked} | Trúng: {stats.available}){Style.RESET_ALL}"
                            )
                            await send_webhook(session, username, tag)
                        else:
                            stats.taken += 1
                            print(
                                f"{Fore.RED}[TAKEN]     [{tag:<6}] {username:<8} "
                                f"{Fore.LIGHTBLACK_EX}(Checked: {stats.checked} | Trúng: {stats.available}){Style.RESET_ALL}"
                            )

                        if not proxy_pool.enabled and stats.delay > NO_PROXY_DELAY:
                            stats.delay = max(NO_PROXY_DELAY, stats.delay - 0.2)
                    return

                elif resp.status == 429:
                    retry_after = 5.0
                    try:
                        data = await resp.json()
                        retry_after = float(data.get("retry_after", 5.0))
                    except Exception:
                        pass

                    async with stats.lock:
                        stats.rate_limits += 1
                        if not proxy_pool.enabled:
                            stats.delay = min(stats.delay + 0.5, 5.0)
                            cur_delay = stats.delay
                        else:
                            cur_delay = 0.0

                    if proxy_pool.enabled:
                        proxy_pool.cooldown(proxy, retry_after + 1.0)
                        print(
                            f"{Fore.YELLOW}[429] Proxy dính giới hạn ({retry_after:.1f}s) -> Đổi proxy...{Style.RESET_ALL}"
                        )
                    else:
                        wait_time = retry_after + 0.5
                        print(
                            f"{Fore.YELLOW}[429] No-Proxy dính rate limit -> Tự nghỉ {wait_time:.1f}s "
                            f"(delay mới: {cur_delay:.1f}s){Style.RESET_ALL}"
                        )
                        await asyncio.sleep(wait_time)

                elif resp.status == 400:
                    async with stats.lock:
                        append_line(CHECKED_FILE, username)
                    print(f"{Fore.MAGENTA}[INVALID]   [{tag:<6}] {username}{Style.RESET_ALL}")
                    return

                else:
                    if proxy_pool.enabled:
                        proxy_pool.cooldown(proxy, 20.0)
                    else:
                        print(f"{Fore.YELLOW}[HTTP {resp.status}] Thử lại sau 3s...{Style.RESET_ALL}")
                        await asyncio.sleep(3.0)

        except (aiohttp.ClientError, asyncio.TimeoutError):
            if proxy_pool.enabled:
                proxy_pool.cooldown(proxy, 15.0)
            else:
                print(f"{Fore.YELLOW}[NETWORK] Lỗi mạng, thử lại sau 3s...{Style.RESET_ALL}")
                await asyncio.sleep(3.0)


async def run_checker(mode_keys: list[str], target_amount: int):
    proxy_pool = ProxyPool(PROXY_FILE)
    seen = load_checked()
    gen = multi_mode_generator(mode_keys, seen)
    stats = Stats()

    selected_labels = ", ".join(MODES[k]["label"] for k in mode_keys)

    if proxy_pool.enabled:
        workers_count = PROXY_CONCURRENCY
        print(
            f"\n{Fore.CYAN}[MODE] PROXY ({len(proxy_pool.proxies)} proxy | {workers_count} luồng)"
            f"\n[LOẠI] {selected_labels}{Style.RESET_ALL}"
        )
    else:
        workers_count = 1
        print(
            f"\n{Fore.CYAN}[MODE] NO-PROXY (tự động kích hoạt vì không có proxy | Delay: {NO_PROXY_DELAY}s)"
            f"\n[LOẠI] {selected_labels}{Style.RESET_ALL}"
        )

    limit_text = "Không giới hạn (Ctrl+C để dừng)" if target_amount == 0 else str(target_amount)
    print(f"{Fore.CYAN}[INFO] Số lượng quét: {limit_text} | Đã lưu trước đó: {len(seen)} tên{Style.RESET_ALL}\n")

    gen_lock = asyncio.Lock()
    dispatched = 0

    async def next_item() -> Optional[tuple[str, str]]:
        nonlocal dispatched
        async with gen_lock:
            if target_amount > 0 and dispatched >= target_amount:
                return None
            try:
                item = next(gen)
                dispatched += 1
                return item
            except StopIteration:
                return None

    async with aiohttp.ClientSession() as session:
        async def worker():
            while True:
                item = await next_item()
                if item is None:
                    return
                username, tag = item
                await check_username(session, username, tag, proxy_pool, stats)
                if not proxy_pool.enabled:
                    await asyncio.sleep(stats.delay)

        try:
            await asyncio.gather(*(asyncio.create_task(worker()) for _ in range(workers_count)))
        finally:
            print(
                f"\n{Fore.CYAN}=== KẾT QUẢ ===\n"
                f"Đã check : {stats.checked}\n"
                f"Còn trống: {stats.available} (lưu tại {AVAILABLE_FILE})\n"
                f"Đã có chủ: {stats.taken}\n"
                f"Rate 429 : {stats.rate_limits}{Style.RESET_ALL}"
            )


def parse_mode_selection(raw: str) -> list[str]:
    cleaned = raw.strip().lower()
    if cleaned in ("all", "tat ca", "tất cả", "*"):
        return list(MODES.keys())

    # Hỗ trợ nhập kiểu: "5, 8, 9" hoặc "4 6 8" hoặc "8"
    tokens = re.split(r"[\s,;+/]+", cleaned)
    selected: list[str] = []
    for t in tokens:
        if t in MODES and t not in selected:
            selected.append(t)
    return selected


def main():
    print(f"{Fore.CYAN}=== DISCORD AUTO-GEN USERNAME CHECKER (MULTI-MODE) ==={Style.RESET_ALL}")
    for key, info in MODES.items():
        print(f" {key}. {info['label']:<7} - {info['desc']}")
    print(" all.       - Chọn tất cả các chế độ trên")

    raw_choice = input("\nChọn 1 hoặc nhiều chế độ (vd: 8 | 4,6,8,9 | all): ")
    selected = parse_mode_selection(raw_choice)
    if not selected:
        print(f"{Fore.RED}Lựa chọn không hợp lệ.{Style.RESET_ALL}")
        return

    amount_input = input("Số lượng muốn check (nhập 0 để chạy liên tục): ").strip()
    target_amount = int(amount_input) if amount_input.isdigit() else 0

    try:
        asyncio.run(run_checker(selected, target_amount))
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Đã dừng tool theo yêu cầu.{Style.RESET_ALL}")


if __name__ == "__main__":
    main()
