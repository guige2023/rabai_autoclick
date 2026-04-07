"""Tests for date utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.date_utils import (
    today,
    yesterday,
    tomorrow,
    parse_date,
    format_date,
    is_weekend,
    is_weekday,
    is_same_day,
    is_today,
    is_past,
    is_future,
    add_days_to_date,
    subtract_days_from_date,
    days_between_dates,
    weeks_between_dates,
    months_between_dates,
    years_between_dates,
    start_of_week,
    end_of_week,
    start_of_month,
    end_of_month,
    start_of_year,
    end_of_year,
    get_week_number,
    get_quarter,
    is_leap_year,
    get_days_in_month,
    date_range,
    get_weekdays,
    get_weekends,
    parse_multiple_dates,
    format_date_range,
    get_age,
    is_valid_date,
    date_to_tuple,
    tuple_to_date,
    date_to_ordinal,
    ordinal_to_date,
    get_nth_weekday_of_month,
    get_next_weekday,
)


class TestToday:
    """Tests for today function."""

    def test_today(self) -> None:
        """Test getting today's date."""
        result = today()
        from datetime import date
        assert isinstance(result, date)


class TestYesterday:
    """Tests for yesterday function."""

    def test_yesterday(self) -> None:
        """Test getting yesterday's date."""
        result = yesterday()
        assert result == today() - __import__('datetime').timedelta(days=1)


class TestTomorrow:
    """Tests for tomorrow function."""

    def test_tomorrow(self) -> None:
        """Test getting tomorrow's date."""
        result = tomorrow()
        assert result == today() + __import__('datetime').timedelta(days=1)


class TestParseDate:
    """Tests for parse_date function."""

    def test_parse_date(self) -> None:
        """Test parsing date."""
        result = parse_date("2024-01-15")
        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_parse_date_invalid(self) -> None:
        """Test parsing invalid date."""
        result = parse_date("not a date")
        assert result is None


class TestFormatDate:
    """Tests for format_date function."""

    def test_format_date(self) -> None:
        """Test formatting date."""
        from datetime import date
        d = date(2024, 1, 15)
        result = format_date(d)
        assert result == "2024-01-15"


class TestIsWeekend:
    """Tests for is_weekend function."""

    def test_is_weekend_saturday(self) -> None:
        """Test Saturday is weekend."""
        from datetime import date
        d = date(2024, 1, 13)
        assert is_weekend(d)

    def test_is_weekday_monday(self) -> None:
        """Test Monday is not weekend."""
        from datetime import date
        d = date(2024, 1, 15)
        assert not is_weekend(d)


class TestIsWeekday:
    """Tests for is_weekday function."""

    def test_is_weekday_monday(self) -> None:
        """Test Monday is weekday."""
        from datetime import date
        d = date(2024, 1, 15)
        assert is_weekday(d)


class TestIsSameDay:
    """Tests for is_same_day function."""

    def test_is_same_day(self) -> None:
        """Test same day detection."""
        from datetime import date
        d1 = date(2024, 1, 15)
        d2 = date(2024, 1, 15)
        assert is_same_day(d1, d2)

    def test_is_different_day(self) -> None:
        """Test different day detection."""
        from datetime import date
        d1 = date(2024, 1, 15)
        d2 = date(2024, 1, 16)
        assert not is_same_day(d1, d2)


class TestIsToday:
    """Tests for is_today function."""

    def test_is_today(self) -> None:
        """Test today detection."""
        assert is_today(today())


class TestIsPast:
    """Tests for is_past function."""

    def test_is_past(self) -> None:
        """Test past detection."""
        past = yesterday()
        assert is_past(past)


class TestIsFuture:
    """Tests for is_future function."""

    def test_is_future(self) -> None:
        """Test future detection."""
        future = tomorrow()
        assert is_future(future)


class TestAddDaysToDate:
    """Tests for add_days_to_date function."""

    def test_add_days(self) -> None:
        """Test adding days."""
        from datetime import date
        d = date(2024, 1, 15)
        result = add_days_to_date(d, 5)
        assert result.day == 20


class TestSubtractDaysFromDate:
    """Tests for subtract_days_from_date function."""

    def test_subtract_days(self) -> None:
        """Test subtracting days."""
        from datetime import date
        d = date(2024, 1, 15)
        result = subtract_days_from_date(d, 5)
        assert result.day == 10


class TestDaysBetweenDates:
    """Tests for days_between_dates function."""

    def test_days_between(self) -> None:
        """Test calculating days between."""
        from datetime import date
        d1 = date(2024, 1, 1)
        d2 = date(2024, 1, 10)
        assert days_between_dates(d1, d2) == 9


class TestWeeksBetweenDates:
    """Tests for weeks_between_dates function."""

    def test_weeks_between(self) -> None:
        """Test calculating weeks between."""
        from datetime import date
        d1 = date(2024, 1, 1)
        d2 = date(2024, 1, 15)
        assert weeks_between_dates(d1, d2) == 2


class TestMonthsBetweenDates:
    """Tests for months_between_dates function."""

    def test_months_between(self) -> None:
        """Test calculating months between."""
        from datetime import date
        d1 = date(2024, 1, 1)
        d2 = date(2024, 6, 1)
        assert months_between_dates(d1, d2) == 5


class TestYearsBetweenDates:
    """Tests for years_between_dates function."""

    def test_years_between(self) -> None:
        """Test calculating years between."""
        from datetime import date
        d1 = date(2020, 1, 1)
        d2 = date(2024, 1, 1)
        assert years_between_dates(d1, d2) == 4


class TestStartOfWeek:
    """Tests for start_of_week function."""

    def test_start_of_week(self) -> None:
        """Test getting start of week."""
        from datetime import date
        d = date(2024, 1, 17)
        result = start_of_week(d)
        assert result.weekday() == 0


class TestEndOfWeek:
    """Tests for end_of_week function."""

    def test_end_of_week(self) -> None:
        """Test getting end of week."""
        from datetime import date
        d = date(2024, 1, 15)
        result = end_of_week(d)
        assert result.weekday() == 6


class TestStartOfMonth:
    """Tests for start_of_month function."""

    def test_start_of_month(self) -> None:
        """Test getting start of month."""
        from datetime import date
        d = date(2024, 1, 15)
        result = start_of_month(d)
        assert result.day == 1


class TestEndOfMonth:
    """Tests for end_of_month function."""

    def test_end_of_month(self) -> None:
        """Test getting end of month."""
        from datetime import date
        d = date(2024, 1, 15)
        result = end_of_month(d)
        assert result.day == 31


class TestStartOfYear:
    """Tests for start_of_year function."""

    def test_start_of_year(self) -> None:
        """Test getting start of year."""
        from datetime import date
        d = date(2024, 6, 15)
        result = start_of_year(d)
        assert result.month == 1
        assert result.day == 1


class TestEndOfYear:
    """Tests for end_of_year function."""

    def test_end_of_year(self) -> None:
        """Test getting end of year."""
        from datetime import date
        d = date(2024, 6, 15)
        result = end_of_year(d)
        assert result.month == 12
        assert result.day == 31


class TestGetWeekNumber:
    """Tests for get_week_number function."""

    def test_get_week_number(self) -> None:
        """Test getting week number."""
        from datetime import date
        d = date(2024, 1, 15)
        result = get_week_number(d)
        assert result > 0


class TestGetQuarter:
    """Tests for get_quarter function."""

    def test_get_quarter(self) -> None:
        """Test getting quarter."""
        from datetime import date
        d = date(2024, 4, 15)
        assert get_quarter(d) == 2


class TestIsLeapYear:
    """Tests for is_leap_year function."""

    def test_is_leap_year(self) -> None:
        """Test leap year detection."""
        assert is_leap_year(2024)
        assert not is_leap_year(2023)


class TestGetDaysInMonth:
    """Tests for get_days_in_month function."""

    def test_days_in_january(self) -> None:
        """Test days in January."""
        assert get_days_in_month(2024, 1) == 31

    def test_days_in_february_leap(self) -> None:
        """Test days in February (leap year)."""
        assert get_days_in_month(2024, 2) == 29

    def test_days_in_february_non_leap(self) -> None:
        """Test days in February (non-leap year)."""
        assert get_days_in_month(2023, 2) == 28


class TestDateRange:
    """Tests for date_range function."""

    def test_date_range(self) -> None:
        """Test generating date range."""
        from datetime import date
        d1 = date(2024, 1, 1)
        d2 = date(2024, 1, 3)
        result = date_range(d1, d2)
        assert len(result) == 3


class TestGetWeekdays:
    """Tests for get_weekdays function."""

    def test_get_weekdays(self) -> None:
        """Test getting weekdays."""
        from datetime import date
        d1 = date(2024, 1, 1)
        d2 = date(2024, 1, 7)
        result = get_weekdays(d1, d2)
        assert all(is_weekday(d) for d in result)


class TestGetWeekends:
    """Tests for get_weekends function."""

    def test_get_weekends(self) -> None:
        """Test getting weekends."""
        from datetime import date
        d1 = date(2024, 1, 1)
        d2 = date(2024, 1, 7)
        result = get_weekends(d1, d2)
        assert all(is_weekend(d) for d in result)


class TestParseMultipleDates:
    """Tests for parse_multiple_dates function."""

    def test_parse_multiple_dates(self) -> None:
        """Test parsing multiple dates."""
        result = parse_multiple_dates("2024-01-01, 2024-01-02")
        assert len(result) == 2


class TestFormatDateRange:
    """Tests for format_date_range function."""

    def test_format_date_range(self) -> None:
        """Test formatting date range."""
        from datetime import date
        d1 = date(2024, 1, 1)
        d2 = date(2024, 1, 3)
        result = format_date_range(d1, d2)
        assert "2024-01-01 to 2024-01-03" in result


class TestGetAge:
    """Tests for get_age function."""

    def test_get_age(self) -> None:
        """Test calculating age."""
        from datetime import date
        birth = date(2000, 1, 1)
        age = get_age(birth)
        assert age >= 24


class TestIsValidDate:
    """Tests for is_valid_date function."""

    def test_valid_date(self) -> None:
        """Test valid date."""
        assert is_valid_date(2024, 1, 15)

    def test_invalid_date(self) -> None:
        """Test invalid date."""
        assert not is_valid_date(2024, 13, 1)


class TestDateToTuple:
    """Tests for date_to_tuple function."""

    def test_date_to_tuple(self) -> None:
        """Test converting date to tuple."""
        from datetime import date
        d = date(2024, 1, 15)
        result = date_to_tuple(d)
        assert result == (2024, 1, 15)


class TestTupleToDate:
    """Tests for tuple_to_date function."""

    def test_tuple_to_date(self) -> None:
        """Test converting tuple to date."""
        result = tuple_to_date((2024, 1, 15))
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15


class TestDateToOrdinal:
    """Tests for date_to_ordinal function."""

    def test_date_to_ordinal(self) -> None:
        """Test converting date to ordinal."""
        from datetime import date
        d = date(2024, 1, 15)
        result = date_to_ordinal(d)
        assert result > 0


class TestOrdinalToDate:
    """Tests for ordinal_to_date function."""

    def test_ordinal_to_date(self) -> None:
        """Test converting ordinal to date."""
        from datetime import date
        d = date(2024, 1, 15)
        ordinal = date_to_ordinal(d)
        result = ordinal_to_date(ordinal)
        assert result == d


class TestGetNthWeekdayOfMonth:
    """Tests for get_nth_weekday_of_month function."""

    def test_get_nth_weekday(self) -> None:
        """Test getting nth weekday."""
        result = get_nth_weekday_of_month(2024, 1, 0, 1)
        assert result is not None
        assert result.weekday() == 0


class TestGetNextWeekday:
    """Tests for get_next_weekday function."""

    def test_get_next_weekday(self) -> None:
        """Test getting next weekday."""
        from datetime import date
        d = date(2024, 1, 15)
        result = get_next_weekday(d, 0)
        assert result.weekday() == 0
        assert result >= d


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
