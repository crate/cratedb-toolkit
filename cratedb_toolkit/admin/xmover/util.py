def format_size(size_gb: float) -> str:
    """Format size in GB with appropriate precision"""
    if size_gb >= 1000:
        return f"{size_gb / 1000:.1f}TB"
    elif size_gb >= 1:
        return f"{size_gb:.1f}GB"
    else:
        return f"{size_gb * 1000:.0f}MB"


def format_percentage(value: float) -> str:
    """Format percentage with color coding"""
    color = "green"
    if value > 80:
        color = "red"
    elif value > 70:
        color = "yellow"
    return f"[{color}]{value:.1f}%[/{color}]"


def format_translog_info(recovery_info) -> str:
    """Format translog size information with color coding"""
    tl_bytes = recovery_info.translog_size_bytes

    # Only show if significant (>10MB for production)
    if tl_bytes < 10 * 1024 * 1024:  # 10MB for production
        return ""

    tl_gb = recovery_info.translog_size_gb

    # Color coding based on size
    if tl_gb >= 5.0:
        color = "red"
    elif tl_gb >= 1.0:
        color = "yellow"
    else:
        color = "green"

    # Format size
    if tl_gb >= 1.0:
        size_str = f"{tl_gb:.1f}GB"
    else:
        size_str = f"{tl_gb * 1000:.0f}MB"

    return f" [dim]([{color}]TL:{size_str}[/{color}])[/dim]"
