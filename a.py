import asyncio
import ipaddress
import itertools
import os
import random
import string
import time
from typing import Iterator, Optional

import aiohttp
from colorama import Fore, Style, init

init(autoreset=True)

# ================= CẤU HÌNH =================
API_URL = "https://discord.com/api/v9/unique-username/username-attempt-unauthed"

# Dán link Webhook Discord để báo về server khi trúng tên trống (để trống nếu không dùng)
WEBHOOK_URL = ""

PROXY_FILE = "proxies.txt"       # Không có file hoặc file trống -> tự chạy NO-PROXY
AVAILABLE_FILE = "available.txt" # Lưu username còn trống
CHECKED_FILE = "checked.txt"     # Lưu username đã check để lần sau không check lại

# Cấu hình tốc độ & check IP
NO_PROXY_DELAY = 1.5             # Delay mặc định (giây) khi chạy No-Proxy
DEFAULT_PROXY_THREADS = 20       # Số luồng mặc định khi có proxy
DEFAULT_NOPROXY_THREADS = 1      # Số luồng mặc định khi No-Proxy
IP_CHECK_INTERVAL = 2.0          # Chu kỳ kiểm tra IP hiện tại (giây) ở chế độ No-Proxy
REQUEST_TIMEOUT = 10

IP_CHECK_URLS = [
    "https://api64.ipify.org",   # Hỗ trợ nhận diện cả IPv4 và IPv6
    "https://api.ipify.org",
    "https://icanhazip.com",
]
# ============================================

LETTERS = string.ascii_lowercase
ALPHANUM = string.ascii_lowercase + string.digits
ALL_CHARS = string.ascii_lowercase + string.digits + "_."


def is_valid_username(name: str) -> bool:
    return 2 <= len(name) <= 32 and ".." not in name


def is_valid_ipv6(addr: str) -> bool:
    try:
        ipaddress.IPv6Address(addr)
        return True
    except ValueError:
        return False


def format_proxy(raw: str) -> Optional[str]:
    """
    Hỗ trợ đa dạng định dạng Proxy IPv4 & IPv6:
    - http://user:pass@ip:port hoặc http://user:pass@[ipv6]:port
    - ip:port hoặc ip:port:user:pass
    - user:pass@ip:port hoặc user:pass@[ipv6]:port
    - [ipv6]:port hoặc [ipv6]:port:user:pass
    - ipv6:port hoặc ipv6:port:user:pass (không ngoặc vuông)
    """
    raw = raw.strip()
    if not raw or raw.startswith("#"):
        return None

    if raw.startswith(("http://", "https://")):
        return raw

    if "@" in raw:
        return f"http://{raw}"

    # Dạng có ngoặc vuông cho IPv6: [2001:db8::1]:8080 hoặc [2001:db8::1]:8080:user:pass
    if raw.startswith("[") and "]" in raw:
        end_idx = raw.index("]")
        host = raw[: end_idx + 1]
        rest = raw[end_idx + 2 :]  # phần sau "]:"
        parts = rest.split(":")
        if len(parts) == 1 and parts[0].isdigit():
            return f"http://{host}:{parts[0]}"
        if len(parts) == 3 and parts[0].isdigit():
            port, user, pwd = parts
            return f"http://{user}:{pwd}@{host}:{port}"
        return f"http://{raw}"

    parts = raw.split(":")

    # IPv4 / domain: host:port
    if len(parts) == 2 and parts[1].isdigit():
        return f"http://{parts[0]}:{parts[1]}"

    # IPv4 / domain: host:port:user:pass
    if len(parts) == 4 and parts[1].isdigit() and not is_valid_ipv6(":".join(parts[:-1])):
        ip, port, user, pwd = parts
        return f"http://{user}:{pwd}@{ip}:{port}"

    # IPv6 không ngoặc vuông: ipv6:port
    if len(parts) >= 3 and parts[-1].isdigit():
        candidate_ip = ":".join(parts[:-1])
        if is_valid_ipv6(candidate_ip):
            return f"http://[{candidate_ip}]:{parts[-1]}"

    # IPv6 không ngoặc vuông kèm auth: ipv6:port:user:pass
    if len(parts) >= 5 and parts[-3].isdigit():
        candidate_ip = ":".join(parts[:-3])
        if is_valid_ipv6(candidate_ip):
            port, user, pwd = parts[-3], parts[-2], parts[-1]
            return f"http://{user}:{pwd}@[{candidate_ip}]:{port}"

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


class NoProxyIPMonitor:
    """Theo dõi IP công khai liên tục ở chế độ No-Proxy. Tự mở khóa ngay khi đổi/fake IP."""

    def __init__(self, stats: "Stats"):
        self.stats = stats
        self.current_ip: Optional[str] = None
        self.unblock_event = asyncio.Event()
        self.unblock_event.set()  # Ban đầu cho phép chạy
        self.resume_at: float = 0.0
        self._stop = False

    async def fetch_ip(self, session: aiohttp.ClientSession) -> Optional[str]:
        for url in IP_CHECK_URLS:
            try:
                # Connection: close để ép mở kết nối mới, phát hiện VPN/Fake IP ngay lập tức
                async with session.get(
                    url,
                    headers={"Connection": "close"},
                    timeout=aiohttp.ClientTimeout(total=4),
                ) as resp:
                    if resp.status == 200:
                        ip = (await resp.text()).strip()
                        if ip:
                            return ip
            except Exception:
                continue
        return None

    async def init_ip(self, session: aiohttp.ClientSession):
        ip = await self.fetch_ip(session)
        self.current_ip = ip or "Unknown"
        print(f"{Fore.CYAN}[IP MONITOR] IP hiện tại: {self.current_ip}{Style.RESET_ALL}")

    def trigger_block(self, retry_after: float, reason: str, hard_block: bool = False):
        if not self.unblock_event.is_set():
            return  # Đang ở trạng thái chờ rồi, tránh in lặp nhiều luồng

        self.unblock_event.clear()
        if hard_block:
            self.resume_at = 0.0  # Chờ tới khi đổi IP mới chạy tiếp
            print(
                f"{Fore.RED}[{reason}] IP {self.current_ip} bị chặn! "
                f"Hãy fake/đổi IP, tool đang check IP liên tục và sẽ tự chạy tiếp...{Style.RESET_ALL}"
            )
        else:
            self.resume_at = time.monotonic() + retry_after
            print(
                f"{Fore.YELLOW}[{reason}] IP {self.current_ip} tạm dừng {retry_after:.1f}s "
                f"(hoặc fake/đổi IP ngay để chạy tiếp không cần chờ)...{Style.RESET_ALL}"
            )

    async def wait_if_blocked(self):
        await self.unblock_event.wait()

    async def run(self, session: aiohttp.ClientSession):
        while not self._stop:
            await asyncio.sleep(IP_CHECK_INTERVAL)
            new_ip = await self.fetch_ip(session)

            if new_ip and self.current_ip and new_ip != self.current_ip:
                old_ip = self.current_ip
                self.current_ip = new_ip
                self.resume_at = 0.0
                self.stats.delay = NO_PROXY_DELAY
                print(
                    f"{Fore.GREEN}[IP ĐÃ ĐỔI] {old_ip} -> {new_ip} | "
                    f"Phát hiện IP mới, tiếp tục check ngay!{Style.RESET_ALL}"
                )
                self.unblock_event.set()
                continue

            # Nếu không đổi IP nhưng đã hết thời gian cooldown 429 thì tự mở lại
            if not self.unblock_event.is_set() and self.resume_at > 0:
                if time.monotonic() >= self.resume_at:
                    self.resume_at = 0.0
                    print(f"{Fore.CYAN}[RESUME] Hết thời gian chờ rate limit -> Chạy tiếp...{Style.RESET_ALL}")
                    self.unblock_event.set()

    def stop(self):
        self._stop = True
        self.unblock_event.set()


def load_checked() -> set[str]:
    if not os.path.exists(CHECKED_FILE):
        return set()
    with open(CHECKED_FILE, "r", encoding="utf-8") as f:
        return {line.strip().lower() for line in f if line.strip()}


def username_generator(length: int, alphabet: str, seen: set[str]) -> Iterator[str]:
    if length <= 3:
        pool = [
            "".join(chars)
            for chars in itertools.product(alphabet, repeat=length)
            if is_valid_username("".join(chars))
        ]
        random.shuffle(pool)
        for name in pool:
            if name not in seen:
                seen.add(name)
                yield name
    else:
        max_space = len(alphabet) ** length
        while len(seen) < max_space:
            name = "".join(random.choices(alphabet, k=length))
            if is_valid_username(name) and name not in seen:
                seen.add(name)
                yield name


class Stats:
    def __init__(self):
        self.checked = 0
        self.available = 0
        self.taken = 0
        self.rate_limits = 0
        self.delay = NO_PROXY_DELAY
        self.lock = asyncio.Lock()


async def send_webhook(session: aiohttp.ClientSession, username: str):
    if not WEBHOOK_URL.strip():
        return
    try:
        await session.post(
            WEBHOOK_URL.strip(),
            json={"content": f"✅ **Username Discord trống:** `{username}`"},
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
    proxy_pool: ProxyPool,
    stats: Stats,
    ip_monitor: Optional[NoProxyIPMonitor],
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
        if not proxy_pool.enabled and ip_monitor:
            await ip_monitor.wait_if_blocked()

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
                                f"{Fore.GREEN}[AVAILABLE] {username:<8} "
                                f"(Checked: {stats.checked} | Trúng: {stats.available}){Style.RESET_ALL}"
                            )
                            await send_webhook(session, username)
                        else:
                            stats.taken += 1
                            print(
                                f"{Fore.RED}[TAKEN]     {username:<8} "
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

                    if proxy_pool.enabled:
                        proxy_pool.cooldown(proxy, retry_after + 1.0)
                        print(
                            f"{Fore.YELLOW}[429] Proxy dính giới hạn ({retry_after:.1f}s) -> Đổi proxy...{Style.RESET_ALL}"
                        )
                    elif ip_monitor:
                        stats.delay = min(stats.delay + 0.5, 5.0)
                        ip_monitor.trigger_block(
                            retry_after=retry_after + 0.5,
                            reason=f"429 RATE LIMIT",
                            hard_block=False,
                        )

                elif resp.status == 403:
                    if proxy_pool.enabled:
                        proxy_pool.cooldown(proxy, 60.0)
                        print(
                            f"{Fore.RED}[403] Proxy bị Cloudflare/Discord chặn -> Đổi proxy (nghỉ 60s)...{Style.RESET_ALL}"
                        )
                    elif ip_monitor:
                        ip_monitor.trigger_block(
                            retry_after=0.0,
                            reason="403 BLOCK IP",
                            hard_block=True,
                        )

                elif resp.status == 400:
                    append_line(CHECKED_FILE, username)
                    print(f"{Fore.MAGENTA}[INVALID]   {username}{Style.RESET_ALL}")
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
                print(f"{Fore.YELLOW}[NETWORK] Mất kết nối / đang đổi IP, thử lại sau 2s...{Style.RESET_ALL}")
                await asyncio.sleep(2.0)


async def run_checker(
    length: int,
    alphabet: str,
    label: str,
    target_amount: int,
    workers_count: int,
    proxy_pool: ProxyPool,
):
    seen = load_checked()
    gen = username_generator(length, alphabet, seen)
    stats = Stats()
    ip_monitor = None if proxy_pool.enabled else NoProxyIPMonitor(stats)

    if proxy_pool.enabled:
        print(
            f"{Fore.CYAN}[MODE] PROXY IPv4/IPv6 ({len(proxy_pool.proxies)} proxy | {workers_count} luồng) "
            f"| Loại: {label}{Style.RESET_ALL}"
        )
    else:
        print(
            f"{Fore.CYAN}[MODE] NO-PROXY + AUTO CHECK IP ({workers_count} luồng) "
            f"| Loại: {label} | Delay: {NO_PROXY_DELAY}s{Style.RESET_ALL}"
        )

    limit_text = "Không giới hạn (Ctrl+C để dừng)" if target_amount == 0 else str(target_amount)
    print(f"{Fore.CYAN}[INFO] Số lượng quét: {limit_text} | Đã lưu trước đó: {len(seen)} tên{Style.RESET_ALL}\n")

    gen_lock = asyncio.Lock()
    dispatched = 0

    async def next_name() -> Optional[str]:
        nonlocal dispatched
        async with gen_lock:
            if target_amount > 0 and dispatched >= target_amount:
                return None
            try:
                name = next(gen)
                dispatched += 1
                return name
            except StopIteration:
                return None

    # family=0 hỗ trợ dual-stack IPv4 & IPv6; limit=0 tránh bị giới hạn luồng cao
    connector = aiohttp.TCPConnector(family=0, limit=0)
    async with aiohttp.ClientSession(connector=connector) as session:
        monitor_task = None
        if ip_monitor:
            await ip_monitor.init_ip(session)
            monitor_task = asyncio.create_task(ip_monitor.run(session))

        async def worker():
            while True:
                name = await next_name()
                if name is None:
                    return
                await check_username(session, name, proxy_pool, stats, ip_monitor)
                if not proxy_pool.enabled:
                    await asyncio.sleep(stats.delay)

        try:
            await asyncio.gather(*(asyncio.create_task(worker()) for _ in range(workers_count)))
        finally:
            if ip_monitor:
                ip_monitor.stop()
            if monitor_task:
                monitor_task.cancel()
            print(
                f"\n{Fore.CYAN}=== KẾT QUẢ ===\n"
                f"Đã check : {stats.checked}\n"
                f"Còn trống: {stats.available} (lưu tại {AVAILABLE_FILE})\n"
                f"Đã có chủ: {stats.taken}\n"
                f"Rate 429 : {stats.rate_limits}{Style.RESET_ALL}"
            )


def main():
    proxy_pool = ProxyPool(PROXY_FILE)
    default_threads = DEFAULT_PROXY_THREADS if proxy_pool.enabled else DEFAULT_NOPROXY_THREADS
    mode_status = f"PROXY ({len(proxy_pool.proxies)} con)" if proxy_pool.enabled else "NO-PROXY (Auto Check IP)"

    print(f"{Fore.CYAN}=== DISCORD AUTO-GEN USERNAME CHECKER ==={Style.RESET_ALL}")
    print(f"{Fore.YELLOW}Trạng thái hiện tại: {mode_status}{Style.RESET_ALL}")
    print("1. 2l  (2 chữ cái)")
    print("2. 2c  (2 ký tự: chữ + số + _ .)")
    print("3. 3l  (3 chữ cái)")
    print("4. 3c  (3 ký tự: chữ + số + _ .)")
    print("5. 4l  (4 chữ cái)")
    print("6. 4c  (4 ký tự: chữ + số + _ .)")
    print("7. 4c  (4 ký tự sạch: chỉ chữ + số, không _ .)")

    modes = {
        "1": (2, LETTERS, "2l"),
        "2": (2, ALL_CHARS, "2c"),
        "3": (3, LETTERS, "3l"),
        "4": (3, ALL_CHARS, "3c"),
        "5": (4, LETTERS, "4l"),
        "6": (4, ALL_CHARS, "4c"),
        "7": (4, ALPHANUM, "4c (chữ+số)"),
    }

    choice = input("\nChọn chế độ (1-7): ").strip()
    if choice not in modes:
        print(f"{Fore.RED}Lựa chọn không hợp lệ.{Style.RESET_ALL}")
        return

    amount_input = input("Số lượng muốn check (nhập 0 để chạy liên tục): ").strip()
    target_amount = int(amount_input) if amount_input.isdigit() else 0

    threads_input = input(f"Nhập số luồng (Enter để mặc định {default_threads}): ").strip()
    workers_count = int(threads_input) if threads_input.isdigit() and int(threads_input) > 0 else default_threads

    length, alphabet, label = modes[choice]
    try:
        asyncio.run(run_checker(length, alphabet, label, target_amount, workers_count, proxy_pool))
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Đã dừng tool theo yêu cầu.{Style.RESET_ALL}")


if __name__ == "__main__":
    main()
