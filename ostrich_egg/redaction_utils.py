def should_redact_along_axis(
    incidence: float | int,
    masked_value_count: int = 0,
    minimum_threshold: int | float = 11,
    is_anonymous: bool = True,
    previous_cell_redacted: bool | None = None,
    run_sum_by_axis: float | int = 0,
) -> bool:
    """
    Given a set of dimensions (i.e., the axis) and some pre-calculated windowed rows,
    determine if the cell needs to be redacted based on the available criteria.

    This will be run iteratively until the dataset consists only of cells that are suppressed to anonymity.
    """
    if not is_anonymous:
        return True  # all non-anonymous cells need redacted.
    if previous_cell_redacted is False:
        return False  # there is no latency in this pass, do not redact.

    # if previous_cell_redacted is True
    if run_sum_by_axis - incidence >= minimum_threshold and masked_value_count >= 2:
        return False  # The window along this axis is sufficiently redacted
    # otherwise, we should redact this cell as the window being surveyed along this axis is insufficiently redacted.
    return previous_cell_redacted is not None
