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

try:
    from aiohttp_socks import ProxyConnector
    HAS_SOCKS = True
except ImportError:
    HAS_SOCKS = False

init(autoreset=True)

# ================= CẤU HÌNH CỐ ĐỊNH =================
CHECK_URL = "https://discord.com/api/v9/unique-username/username-attempt-unauthed"
PROXY_TEST_URL = "https://discord.com/api/v9/gateway"  # Endpoint nhẹ để test proxy qua được Cloudflare Discord

WEBHOOK_URL = ""                  # Dán link Webhook Discord nếu muốn nhận thông báo trúng tên trống
AVAILABLE_FILE = "available.txt"  # File DUY NHẤT được tạo ra: chỉ lưu username dùng được

NO_PROXY_DELAY = 1.5              # Delay mặc định (giây) khi chạy No-Proxy
PROXY_TEST_TIMEOUT = 4.5          # Timeout khi test live/die proxy
CHECK_TIMEOUT = 8.0               # Timeout khi check username
MAX_PROXY_FAILS = 2               # Proxy lỗi liên tiếp X lần lúc đang check -> tự xóa khỏi vòng xoay
# ====================================================

VOWELS = "aeiou"
CONSONANTS = "bcdfghjklmnprstvwxyz"
SOFT_ENDINGS = ("nd", "nt", "st", "rk", "lt", "sk", "ck", "ll", "ss", "nn", "th", "ch", "sh")
DIGITS = string.digits
SYMBOLS = "_."

LETTERS = string.ascii_lowercase
ALPHANUM = string.ascii_lowercase + string.digits
ALL_CHARS = string.ascii_lowercase + string.digits + "_."

HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
}


def is_valid_username(name: str) -> bool:
    return 2 <= len(name) <= 32 and ".." not in name


def load_existing_available() -> set[str]:
    """Đọc sẵn available.txt (nếu có) vào RAM để không lưu trùng."""
    if not os.path.exists(AVAILABLE_FILE):
        return set()
    with open(AVAILABLE_FILE, "r", encoding="utf-8") as f:
        return {line.strip().lower() for line in f if line.strip()}


# ================= XỬ LÝ & XOAY PROXY =================
def normalize_proxy_candidates(raw: str, default_proto: str) -> list[str]:
    """
    Chuẩn hóa 1 dòng proxy thành danh sách URL cần thử.
    Hỗ trợ:
      - protocol://ip:port | protocol://user:pass@ip:port
      - ip:port | user:pass@ip:port | ip:port:user:pass
    """
    line = raw.strip()
    if not line or line.startswith("#"):
        return []

    lower = line.lower()
    if lower.startswith(("http://", "https://", "socks4://", "socks5://", "socks4a://", "socks5h://")):
        return [line]

    # Hỗ trợ dạng ip:port:user:pass
    parts = line.split(":")
    if len(parts) == 4 and "@" not in line:
        ip, port, user, pwd = parts
        host_part = f"{user}:{pwd}@{ip}:{port}"
    else:
        host_part = line

    if default_proto == "auto":
        protos = ["http", "socks5", "socks4"] if HAS_SOCKS else ["http"]
    else:
        protos = [default_proto]

    return [f"{p}://{host_part}" for p in protos]


def load_raw_proxies(filepath: str) -> list[str]:
    if not filepath or not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        lines = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
    # Lọc trùng nhưng giữ sạch
    unique = list(dict.fromkeys(lines))
    random.shuffle(unique)
    return unique


class LiveProxyPool:
    def __init__(self, raw_lines: list[str], default_proto: str):
        self.raw_lines = raw_lines
        self.default_proto = default_proto
        self.live: list[str] = []
        self.live_set: set[str] = set()
        self.cooldowns: dict[str, float] = {}
        self.fails: dict[str, int] = {}
        self._rr_index = 0

        self.tested = 0
        self.dead = 0
        self.total_raw = len(raw_lines)
        self.scan_done = len(raw_lines) == 0
        self.has_live_event = asyncio.Event()

    @property
    def enabled(self) -> bool:
        return self.total_raw > 0

    def add_live(self, proxy_url: str):
        if proxy_url not in self.live_set:
            self.live_set.add(proxy_url)
            self.live.append(proxy_url)
            self.fails[proxy_url] = 0
            self.has_live_event.set()

    def mark_success(self, proxy_url: str):
        if proxy_url in self.fails:
            self.fails[proxy_url] = 0

    def mark_cooldown(self, proxy_url: str, seconds: float):
        if proxy_url in self.live_set:
            self.cooldowns[proxy_url] = time.monotonic() + seconds

    def mark_fail(self, proxy_url: str, hard_drop: bool = False):
        if proxy_url not in self.live_set:
            return
        self.fails[proxy_url] = self.fails.get(proxy_url, 0) + 1
        if hard_drop or self.fails[proxy_url] >= MAX_PROXY_FAILS:
            self.live_set.discard(proxy_url)
            try:
                self.live.remove(proxy_url)
            except ValueError:
                pass
            self.cooldowns.pop(proxy_url, None)
            self.fails.pop(proxy_url, None)
            self.dead += 1
            if not self.live:
                self.has_live_event.clear()

    def get_next(self) -> tuple[Optional[str], bool]:
        """
        Trả về (proxy_url, exhausted).
        - exhausted=True khi đã test hết file và không còn proxy live nào.
        - proxy_url=None, exhausted=False khi đang chờ proxy live mới hoặc tất cả đang cooldown.
        """
        if not self.live:
            return None, self.scan_done

        now = time.monotonic()
        n = len(self.live)
        for _ in range(n):
            self._rr_index %= len(self.live)
            proxy = self.live[self._rr_index]
            self._rr_index = (self._rr_index + 1) % len(self.live)
            if self.cooldowns.get(proxy, 0.0) <= now:
                return proxy, False

        return None, False


async def test_single_proxy_url(shared_session: aiohttp.ClientSession, proxy_url: str) -> bool:
    """Test 1 URL proxy xem có kết nối được tới Discord API không."""
    is_socks = proxy_url.lower().startswith(("socks4://", "socks5://", "socks4a://", "socks5h://"))
    timeout = aiohttp.ClientTimeout(total=PROXY_TEST_TIMEOUT)

    try:
        if is_socks:
            if not HAS_SOCKS:
                return False
            connector = ProxyConnector.from_url(proxy_url)
            async with aiohttp.ClientSession(connector=connector) as socks_session:
                async with socks_session.get(PROXY_TEST_URL, headers=HEADERS, timeout=timeout) as resp:
                    return resp.status == 200
        else:
            async with shared_session.get(
                PROXY_TEST_URL, headers=HEADERS, proxy=proxy_url, timeout=timeout
            ) as resp:
                return resp.status == 200
    except Exception:
        return False


async def background_proxy_checker(pool: LiveProxyPool, shared_session: aiohttp.ClientSession, test_workers: int):
    """Chạy ngầm test toàn bộ list proxy, thấy con nào live là đẩy ngay vào vòng xoay."""
    queue: asyncio.Queue[str] = asyncio.Queue()
    for line in pool.raw_lines:
        queue.put_nowait(line)

    async def tester():
        while True:
            try:
                raw = queue.get_nowait()
            except asyncio.QueueEmpty:
                return

            candidates = normalize_proxy_candidates(raw, pool.default_proto)
            found_live = False
            for url in candidates:
                if await test_single_proxy_url(shared_session, url):
                    pool.add_live(url)
                    found_live = True
                    break

            pool.tested += 1
            if not found_live:
                pool.dead += 1
            queue.task_done()

    tasks = [asyncio.create_task(tester()) for _ in range(min(test_workers, max(1, pool.total_raw)))]
    try:
        await asyncio.gather(*tasks)
    finally:
        pool.scan_done = True
        pool.has_live_event.set()  # Đánh thức các worker đang chờ nếu list die sạch


# ================= BỘ SINH USERNAME =================
def make_pronounceable_4l() -> str:
    pattern = random.choices(["CVCV", "VCVC", "CVVC", "CV_END"], weights=[45, 25, 15, 15], k=1)[0]
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
        return f"{c()}{v()}{c()}{d()}"
    if pattern == "CVC_S":
        return f"{c()}{v()}{c()}{s()}"
    if pattern == "S_CVC":
        return f"{s()}{c()}{v()}{c()}"
    if pattern == "CV_S_C":
        return f"{c()}{v()}{s()}{c()}"
    if pattern == "C_S_VC":
        return f"{c()}{s()}{v()}{c()}"
    return f"{c()}{random.choice('0134')}{c()}{v()}"


MODES = {
    "1": {"label": "2l",     "desc": "2 chữ cái (vd: ax, kp)",                 "kind": "exhaustive", "length": 2, "alphabet": LETTERS},
    "2": {"label": "2c",     "desc": "2 ký tự: chữ + số + _ .",                "kind": "exhaustive", "length": 2, "alphabet": ALL_CHARS},
    "3": {"label": "3l",     "desc": "3 chữ cái (vd: kax, miv)",               "kind": "exhaustive", "length": 3, "alphabet": LETTERS},
    "4": {"label": "3c",     "desc": "3 ký tự: chữ + số + _ .",                "kind": "exhaustive", "length": 3, "alphabet": ALL_CHARS},
    "5": {"label": "4l",     "desc": "4 chữ cái ngẫu nhiên",                   "kind": "random",     "length": 4, "alphabet": LETTERS},
    "6": {"label": "4c",     "desc": "4 ký tự: chữ + số + _ .",                "kind": "random",     "length": 4, "alphabet": ALL_CHARS},
    "7": {"label": "4c-an",  "desc": "4 ký tự sạch: chỉ chữ + số (không _ .)", "kind": "random",     "length": 4, "alphabet": ALPHANUM},
    "8": {"label": "4l-dep", "desc": "4l dễ đọc (xen kẽ phụ âm - nguyên âm)",  "kind": "pronounceable_4l"},
    "9": {"label": "4c-dep", "desc": "4c dễ đọc (gốc 3 chữ đẹp + số / _ .)",   "kind": "pronounceable_4c"},
}


def single_mode_generator(mode_key: str, seen: set[str]) -> Iterator[tuple[str, str]]:
    spec = MODES[mode_key]
    label = spec["label"]
    kind = spec["kind"]

    if kind == "exhaustive":
        pool = [
            "".join(chars)
            for chars in itertools.product(spec["alphabet"], repeat=spec["length"])
            if is_valid_username("".join(chars))
        ]
        random.shuffle(pool)
        for name in pool:
            if name not in seen:
                seen.add(name)
                yield name, label
    else:
        retries = 0
        while retries < 5000:
            if kind == "random":
                name = "".join(random.choices(spec["alphabet"], k=spec["length"]))
            elif kind == "pronounceable_4l":
                name = make_pronounceable_4l()
            else:
                name = make_pronounceable_4c()

            if is_valid_username(name) and name not in seen:
                seen.add(name)
                retries = 0
                yield name, label
            else:
                retries += 1


def multi_mode_generator(mode_keys: list[str], seen: set[str]) -> Iterator[tuple[str, str]]:
    active_gens = [single_mode_generator(k, seen) for k in mode_keys]
    while active_gens:
        idx = random.randrange(len(active_gens))
        try:
            yield next(active_gens[idx])
        except StopIteration:
            active_gens.pop(idx)


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
            timeout=aiohttp.ClientTimeout(total=8),
        )
    except Exception:
        pass


async def post_check_request(
    shared_session: aiohttp.ClientSession,
    username: str,
    proxy_url: Optional[str],
) -> tuple[int, dict]:
    """Gửi request check qua No-Proxy, HTTP/HTTPS hoặc SOCKS4/SOCKS5."""
    timeout = aiohttp.ClientTimeout(total=CHECK_TIMEOUT)
    payload = {"username": username}

    if proxy_url and proxy_url.lower().startswith(("socks4://", "socks5://", "socks4a://", "socks5h://")):
        connector = ProxyConnector.from_url(proxy_url)
        async with aiohttp.ClientSession(connector=connector) as socks_session:
            async with socks_session.post(CHECK_URL, json=payload, headers=HEADERS, timeout=timeout) as resp:
                try:
                    data = await resp.json()
                except Exception:
                    data = {}
                return resp.status, data
    else:
        async with shared_session.post(
            CHECK_URL, json=payload, headers=HEADERS, proxy=proxy_url, timeout=timeout
        ) as resp:
            try:
                data = await resp.json()
            except Exception:
                data = {}
            return resp.status, data


async def check_username(
    shared_session: aiohttp.ClientSession,
    username: str,
    tag: str,
    pool: LiveProxyPool,
    stats: Stats,
) -> bool:
    """Trả về False nếu toàn bộ proxy đã die sạch (cần dừng worker)."""
    while True:
        proxy_url: Optional[str] = None
        if pool.enabled:
            proxy_url, exhausted = pool.get_next()
            if exhausted:
                return False
            if proxy_url is None:
                await asyncio.sleep(0.5)
                continue

        try:
            status, data = await post_check_request(shared_session, username, proxy_url)

            if status == 200:
                if proxy_url:
                    pool.mark_success(proxy_url)

                taken = data.get("taken", True)
                is_hit = False

                async with stats.lock:
                    stats.checked += 1
                    proxy_info = (
                        f" | Proxy Live: {len(pool.live)} ({pool.tested}/{pool.total_raw})"
                        if pool.enabled
                        else ""
                    )

                    if not taken:
                        is_hit = True
                        stats.available += 1
                        with open(AVAILABLE_FILE, "a", encoding="utf-8") as f:
                            f.write(username + "\n")
                        print(
                            f"{Fore.GREEN}[AVAILABLE] [{tag:<6}] {username:<8} "
                            f"(Checked: {stats.checked} | Trúng: {stats.available}{proxy_info}){Style.RESET_ALL}"
                        )
                    else:
                        stats.taken += 1
                        print(
                            f"{Fore.RED}[TAKEN]     [{tag:<6}] {username:<8} "
                            f"{Fore.LIGHTBLACK_EX}(Checked: {stats.checked} | Trúng: {stats.available}{proxy_info}){Style.RESET_ALL}"
                        )

                    if not pool.enabled and stats.delay > NO_PROXY_DELAY:
                        stats.delay = max(NO_PROXY_DELAY, stats.delay - 0.2)

                if is_hit:
                    await send_webhook(shared_session, username, tag)
                return True

            elif status == 429:
                retry_after = float(data.get("retry_after", 5.0))
                async with stats.lock:
                    stats.rate_limits += 1
                    if not pool.enabled:
                        stats.delay = min(stats.delay + 0.5, 5.0)
                        cur_delay = stats.delay
                    else:
                        cur_delay = 0.0

                if proxy_url:
                    pool.mark_cooldown(proxy_url, retry_after + 1.0)
                else:
                    wait_time = retry_after + 0.5
                    print(
                        f"{Fore.YELLOW}[429] No-Proxy dính giới hạn -> nghỉ {wait_time:.1f}s "
                        f"(delay mới: {cur_delay:.1f}s){Style.RESET_ALL}"
                    )
                    await asyncio.sleep(wait_time)

            elif status == 400:
                print(f"{Fore.MAGENTA}[INVALID]   [{tag:<6}] {username}{Style.RESET_ALL}")
                return True

            elif status in (401, 403):
                if proxy_url:
                    pool.mark_fail(proxy_url, hard_drop=True)  # Bị Cloudflare/Discord chặn -> loại luôn
                else:
                    print(f"{Fore.YELLOW}[HTTP {status}] IP tạm bị chặn, chờ 5s...{Style.RESET_ALL}")
                    await asyncio.sleep(5.0)

            else:
                if proxy_url:
                    pool.mark_fail(proxy_url)
                else:
                    await asyncio.sleep(2.0)

        except Exception:
            if proxy_url:
                pool.mark_fail(proxy_url)
            else:
                print(f"{Fore.YELLOW}[NETWORK] Lỗi mạng, thử lại sau 3s...{Style.RESET_ALL}")
                await asyncio.sleep(3.0)


async def run_checker(
    mode_keys: list[str],
    target_amount: int,
    threads: int,
    proxy_file: str,
    default_proto: str,
):
    raw_proxies = load_raw_proxies(proxy_file)
    pool = LiveProxyPool(raw_proxies, default_proto)
    seen = load_existing_available()
    gen = multi_mode_generator(mode_keys, seen)
    stats = Stats()

    selected_labels = ", ".join(MODES[k]["label"] for k in mode_keys)
    limit_text = "Không giới hạn (Ctrl+C để dừng)" if target_amount == 0 else str(target_amount)

    print(f"\n{Fore.CYAN}================ BẮT ĐẦU ================{Style.RESET_ALL}")
    print(f"{Fore.CYAN}[LOẠI]      {selected_labels}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}[SỐ LƯỢNG]  {limit_text}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}[LƯU FILE]  Chỉ lưu username dùng được -> {AVAILABLE_FILE}{Style.RESET_ALL}")

    if pool.enabled:
        print(
            f"{Fore.CYAN}[MODE]      FULL PROXY ({pool.total_raw} proxy raw | Protocol: {default_proto.upper()} | "
            f"{threads} luồng check){Style.RESET_ALL}"
        )
        print(f"{Fore.YELLOW}[INFO]      Đang vừa test live/die proxy ngầm vừa xoay check...{Style.RESET_ALL}\n")
    else:
        threads = min(threads, 3)  # Giữ an toàn nếu người dùng nhập luồng quá cao khi chạy No-Proxy
        print(
            f"{Fore.CYAN}[MODE]      NO-PROXY (không có proxy -> tự chạy không proxy | "
            f"{threads} luồng | Delay: {NO_PROXY_DELAY}s){Style.RESET_ALL}\n"
        )

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

    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as shared_session:
        validator_task = None

        if pool.enabled:
            test_workers = min(150, max(50, threads * 2))
            validator_task = asyncio.create_task(
                background_proxy_checker(pool, shared_session, test_workers)
            )

            # Chờ tìm thấy ít nhất 1 proxy live đầu tiên (vẫn in tiến độ gọn)
            while not pool.live and not pool.scan_done:
                print(
                    f"\r{Fore.YELLOW}[PROXY SCAN] Đã test: {pool.tested}/{pool.total_raw} | "
                    f"Live: {len(pool.live)} | Die: {pool.dead}...{Style.RESET_ALL}",
                    end="",
                    flush=True,
                )
                try:
                    await asyncio.wait_for(pool.has_live_event.wait(), timeout=0.5)
                except asyncio.TimeoutError:
                    pass
            print()

            if not pool.live and pool.scan_done:
                print(f"{Fore.RED}[LỖI] Toàn bộ {pool.total_raw} proxy trong file đều DIE hoặc bị Discord chặn!{Style.RESET_ALL}")
                return

        async def worker():
            while True:
                item = await next_item()
                if item is None:
                    return
                username, tag = item
                ok = await check_username(shared_session, username, tag, pool, stats)
                if not ok:
                    return
                if not pool.enabled:
                    await asyncio.sleep(stats.delay)

        try:
            await asyncio.gather(*(asyncio.create_task(worker()) for _ in range(threads)))
        finally:
            if validator_task and not validator_task.done():
                validator_task.cancel()
                try:
                    await validator_task
                except asyncio.CancelledError:
                    pass

            print(
                f"\n{Fore.CYAN}=== KẾT QUẢ ===\n"
                f"Đã check : {stats.checked}\n"
                f"Dùng được: {stats.available} (lưu tại {AVAILABLE_FILE})\n"
                f"Đã có chủ: {stats.taken}\n"
                f"Rate 429 : {stats.rate_limits}{Style.RESET_ALL}"
            )
            if pool.enabled:
                print(
                    f"{Fore.CYAN}Proxy    : Đã test {pool.tested}/{pool.total_raw} | "
                    f"Đang live: {len(pool.live)} | Die/Drop: {pool.dead}{Style.RESET_ALL}"
                )


def parse_mode_selection(raw: str) -> list[str]:
    cleaned = raw.strip().lower()
    if cleaned in ("all", "tat ca", "tất cả", "*"):
        return list(MODES.keys())
    tokens = re.split(r"[\s,;+/]+", cleaned)
    selected: list[str] = []
    for t in tokens:
        if t in MODES and t not in selected:
            selected.append(t)
    return selected


def main():
    print(f"{Fore.CYAN}=== DISCORD AUTO-GEN USERNAME CHECKER (FULL PROXY + LIVE ROTATE) ==={Style.RESET_ALL}")
    for key, info in MODES.items():
        print(f" {key}. {info['label']:<7} - {info['desc']}")
    print(" all.       - Chọn tất cả các chế độ trên")

    raw_choice = input("\n1) Chọn 1 hoặc nhiều chế độ (vd: 8 | 4,6,8,9 | all): ")
    selected = parse_mode_selection(raw_choice)
    if not selected:
        print(f"{Fore.RED}Lựa chọn không hợp lệ.{Style.RESET_ALL}")
        return

    amount_input = input("2) Số lượng username muốn check (nhập 0 để chạy liên tục): ").strip()
    target_amount = int(amount_input) if amount_input.isdigit() else 0

    proxy_file = input("3) Đường dẫn file proxy .txt (nhấn Enter nếu chạy No-Proxy): ").strip().strip('"').strip("'")

    default_proto = "auto"
    if proxy_file:
        if not os.path.exists(proxy_file):
            print(f"{Fore.YELLOW}[CẢNH BÁO] Không tìm thấy file '{proxy_file}' -> Tự chuyển sang NO-PROXY.{Style.RESET_ALL}")
            proxy_file = ""
        else:
            print("   Chọn loại proxy cho các dòng không có tiền tố (vd: ip:port):")
            print("   1. Auto (tự dò HTTP -> SOCKS5 -> SOCKS4)  2. HTTP/HTTPS  3. SOCKS4  4. SOCKS5")
            p_choice = input("   Chọn (1-4, mặc định 1): ").strip()
            proto_map = {"1": "auto", "2": "http", "3": "socks4", "4": "socks5"}
            default_proto = proto_map.get(p_choice, "auto")
            if default_proto in ("socks4", "socks5", "auto") and not HAS_SOCKS:
                print(f"{Fore.YELLOW}[LƯU Ý] Chưa cài 'aiohttp-socks' -> Chỉ chạy được HTTP/HTTPS. Cài bằng: pip install aiohttp-socks{Style.RESET_ALL}")

    default_threads = 30 if proxy_file else 1
    threads_input = input(f"4) Nhập số luồng (mặc định {default_threads}): ").strip()
    threads = int(threads_input) if threads_input.isdigit() and int(threads_input) > 0 else default_threads

    try:
        asyncio.run(run_checker(selected, target_amount, threads, proxy_file, default_proto))
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Đã dừng tool theo yêu cầu.{Style.RESET_ALL}")


if __name__ == "__main__":
    main()
