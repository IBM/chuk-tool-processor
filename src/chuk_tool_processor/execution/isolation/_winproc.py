# chuk_tool_processor/execution/isolation/_winproc.py
"""
Win32 AppContainer process launch + Job Object (EXPERIMENTAL, Windows-only).

Launches a child process as an AppContainer (via a STARTUPINFOEX security-
capabilities attribute) inside a Job Object that caps memory / active processes
and kills the tree on close. All Win32 access is through ``ctypes`` and confined
to :func:`launch_appcontainer`, so importing this module is harmless on any OS.

Verified only via Windows CI; structural first draft.
"""

from __future__ import annotations

from typing import Any

# Win32 constants
_EXTENDED_STARTUPINFO_PRESENT = 0x00080000
_CREATE_SUSPENDED = 0x00000004
_CREATE_NO_WINDOW = 0x08000000
_STARTF_USESTDHANDLES = 0x00000100
_PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES = 0x00020009
_JOB_OBJECT_LIMIT_ACTIVE_PROCESS = 0x00000008
_JOB_OBJECT_LIMIT_PROCESS_MEMORY = 0x00000100
_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
_JobObjectExtendedLimitInformation = 9
_WAIT_TIMEOUT = 0x00000102
_GENERIC_WRITE = 0x40000000
_FILE_SHARE_READ = 0x00000001
_FILE_SHARE_WRITE = 0x00000002
_CREATE_ALWAYS = 2
_INVALID_HANDLE_VALUE = -1


def _quote(arg: str) -> str:
    if arg and not any(c in arg for c in ' \t"'):
        return arg
    return '"' + arg.replace("\\", "\\\\").replace('"', '\\"') + '"'


def launch_appcontainer(
    *,
    argv: list[str],
    sid: Any,
    limits: Any,
    stdout_path: str,
    stderr_path: str,
    cwd: str,
    _ctypes: Any,
) -> tuple[bool, int]:
    """Launch ``argv`` as an AppContainer in a Job Object. Returns (timed_out, exit_code)."""
    import ctypes
    from ctypes import wintypes

    k32 = ctypes.WinDLL("kernel32", use_last_error=True)

    class SECURITY_CAPABILITIES(ctypes.Structure):
        _fields_ = [
            ("AppContainerSid", ctypes.c_void_p),
            ("Capabilities", ctypes.c_void_p),
            ("CapabilityCount", wintypes.DWORD),
            ("Reserved", wintypes.DWORD),
        ]

    class STARTUPINFOW(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("lpReserved", wintypes.LPWSTR),
            ("lpDesktop", wintypes.LPWSTR),
            ("lpTitle", wintypes.LPWSTR),
            ("dwX", wintypes.DWORD),
            ("dwY", wintypes.DWORD),
            ("dwXSize", wintypes.DWORD),
            ("dwYSize", wintypes.DWORD),
            ("dwXCountChars", wintypes.DWORD),
            ("dwYCountChars", wintypes.DWORD),
            ("dwFillAttribute", wintypes.DWORD),
            ("dwFlags", wintypes.DWORD),
            ("wShowWindow", wintypes.WORD),
            ("cbReserved2", wintypes.WORD),
            ("lpReserved2", ctypes.c_void_p),
            ("hStdInput", wintypes.HANDLE),
            ("hStdOutput", wintypes.HANDLE),
            ("hStdError", wintypes.HANDLE),
        ]

    class STARTUPINFOEXW(ctypes.Structure):
        _fields_ = [("StartupInfo", STARTUPINFOW), ("lpAttributeList", ctypes.c_void_p)]

    class PROCESS_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("hProcess", wintypes.HANDLE),
            ("hThread", wintypes.HANDLE),
            ("dwProcessId", wintypes.DWORD),
            ("dwThreadId", wintypes.DWORD),
        ]

    class SECURITY_ATTRIBUTES(ctypes.Structure):
        _fields_ = [
            ("nLength", wintypes.DWORD),
            ("lpSecurityDescriptor", ctypes.c_void_p),
            ("bInheritHandle", wintypes.BOOL),
        ]

    class IO_COUNTERS(ctypes.Structure):
        _fields_ = [
            (n, ctypes.c_ulonglong) for n in ("ReadOp", "WriteOp", "OtherOp", "ReadXfer", "WriteXfer", "OtherXfer")
        ]

    class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_int64),
            ("PerJobUserTimeLimit", ctypes.c_int64),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
            ("IoInfo", IO_COUNTERS),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    def _inheritable_file(path: str) -> Any:
        sa = SECURITY_ATTRIBUTES(ctypes.sizeof(SECURITY_ATTRIBUTES), None, True)
        h = k32.CreateFileW(
            ctypes.c_wchar_p(path),
            _GENERIC_WRITE,
            _FILE_SHARE_READ | _FILE_SHARE_WRITE,
            ctypes.byref(sa),
            _CREATE_ALWAYS,
            0,
            None,
        )
        if h == _INVALID_HANDLE_VALUE:
            raise ctypes.WinError(ctypes.get_last_error())
        return h

    handles_to_close: list[Any] = []
    attr_list = None
    hjob = None
    hout = herr = None
    try:
        # Build the security-capabilities attribute list.
        size = ctypes.c_size_t(0)
        k32.InitializeProcThreadAttributeList(None, 1, 0, ctypes.byref(size))
        buf = (ctypes.c_byte * size.value)()
        attr_list = ctypes.cast(buf, ctypes.c_void_p)
        if not k32.InitializeProcThreadAttributeList(attr_list, 1, 0, ctypes.byref(size)):
            raise ctypes.WinError(ctypes.get_last_error())
        caps = SECURITY_CAPABILITIES(sid, None, 0, 0)
        if not k32.UpdateProcThreadAttribute(
            attr_list,
            0,
            _PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES,
            ctypes.byref(caps),
            ctypes.sizeof(caps),
            None,
            None,
        ):
            raise ctypes.WinError(ctypes.get_last_error())

        hout = _inheritable_file(stdout_path)
        herr = _inheritable_file(stderr_path)
        handles_to_close += [hout, herr]

        si = STARTUPINFOEXW()
        si.StartupInfo.cb = ctypes.sizeof(STARTUPINFOEXW)
        si.StartupInfo.dwFlags = _STARTF_USESTDHANDLES
        si.StartupInfo.hStdInput = None
        si.StartupInfo.hStdOutput = hout
        si.StartupInfo.hStdError = herr
        si.lpAttributeList = attr_list

        pi = PROCESS_INFORMATION()
        cmdline = " ".join(_quote(a) for a in argv)
        flags = _EXTENDED_STARTUPINFO_PRESENT | _CREATE_SUSPENDED | _CREATE_NO_WINDOW
        if not k32.CreateProcessW(
            None,
            ctypes.c_wchar_p(cmdline),
            None,
            None,
            True,
            flags,
            None,
            ctypes.c_wchar_p(cwd),
            ctypes.byref(si.StartupInfo),
            ctypes.byref(pi),
        ):
            raise ctypes.WinError(ctypes.get_last_error())
        handles_to_close += [pi.hProcess, pi.hThread]

        # Job object with memory / process-count caps; kill tree on close.
        hjob = k32.CreateJobObjectW(None, None)
        info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
        flags_lim = _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if limits.max_processes:
            flags_lim |= _JOB_OBJECT_LIMIT_ACTIVE_PROCESS
            info.BasicLimitInformation.ActiveProcessLimit = int(limits.max_processes)
        if limits.memory_bytes:
            flags_lim |= _JOB_OBJECT_LIMIT_PROCESS_MEMORY
            info.ProcessMemoryLimit = int(limits.memory_bytes)
        info.BasicLimitInformation.LimitFlags = flags_lim
        k32.SetInformationJobObject(hjob, _JobObjectExtendedLimitInformation, ctypes.byref(info), ctypes.sizeof(info))
        k32.AssignProcessToJobObject(hjob, pi.hProcess)

        k32.ResumeThread(pi.hThread)

        timeout_ms = int(max(1.0, limits.wall_timeout) * 1000)
        wait = k32.WaitForSingleObject(pi.hProcess, timeout_ms)
        if wait == _WAIT_TIMEOUT:
            k32.TerminateJobObject(hjob, 1)
            k32.WaitForSingleObject(pi.hProcess, 5000)
            return True, 1

        code = wintypes.DWORD(0)
        k32.GetExitCodeProcess(pi.hProcess, ctypes.byref(code))
        return False, int(code.value)
    finally:
        if attr_list is not None:
            k32.DeleteProcThreadAttributeList(attr_list)
        for h in handles_to_close:
            if h:
                k32.CloseHandle(h)
        if hjob:  # closing the job kills any survivors (KILL_ON_JOB_CLOSE)
            k32.CloseHandle(hjob)
