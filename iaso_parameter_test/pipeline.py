from openhexa.sdk import IASOConnection, current_run, parameter, pipeline
from openhexa.sdk.pipelines.parameter import IASOWidget


@pipeline("iaso_parameter_test")
@parameter("iaso_connection", name="IASO connection", type=IASOConnection, required=True)
@parameter(
    "project",
    name="Projects",
    type=int,
    widget=IASOWidget.IASO_PROJECTS,
    connection="iaso_connection",
    required=True,
)
@parameter(
    "form",
    name="Form",
    type=int,
    widget=IASOWidget.IASO_FORMS,
    connection="iaso_connection",
    required=True,
)
@parameter(
    "org_unit",
    name="Organisation Unit",
    type=int,
    widget=IASOWidget.IASO_ORG_UNITS,
    connection="iaso_connection",
    multiple=True,
    required=False,
)
def iaso_parameter_test(
    iaso_connection: IASOConnection, project: int, form: int, org_unit: list[int]
):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive
    computations.
    """
    current_run.log_info(
        f"Form: {form}, project: {project}, org_unit: {', '.join(map(str, org_unit))}"
    )


if __name__ == "__main__":
    iaso_parameter_test()
