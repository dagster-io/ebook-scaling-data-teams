from tests.fixtures import defs  # noqa: F401


def test_project(defs):  # noqa: F811
    assert defs

    # Check schedules
    assert defs.get_schedule_def("sling_schedule")

    # Check sensors
    assert defs.get_sensor_def("freshness_checks_sensor")
