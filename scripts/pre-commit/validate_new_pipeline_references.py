#
# Copyright (c) 2025 by Delphix. All rights reserved.
#

import logging
import pathlib
import re
import typing as tp
import subprocess
import sys
import helpers

TEMPLATE_DIR_REGEX = r"^dcsazure_(\w+)_to_(\w+)_(mask|discovery)_pl$"
FILES_TO_VALIDATE = [
    helpers.CHANGELOG_FILE,
    helpers.README_FILE,
    helpers.DOCKER_COMPOSE_FILE,
    helpers.DOCUMENTATION_FILE,
]

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger("validate_new_pipeline_references")


class ValidationError(Exception):
    """Custom exception for validation-related errors."""


def filter_pipeline_directory(path_files: tp.List[pathlib.Path]) -> set[pathlib.Path]:
    """
    Filter and return the set of pipeline directories from the given list of filepath.
    """
    return {
        pathlib.Path(path.parts[0])
        for path in path_files
        if re.match(TEMPLATE_DIR_REGEX, path.parts[0])
    }


def get_pipeline_files_from_origin_main() -> set[pathlib.Path]:
    """
    Get the list of files in the current working directory
    """
    origin_main_files = helpers.get_files_from_origin_main()
    return filter_pipeline_directory(origin_main_files)


def __validate_pipeline_reference_in_file(file_path: pathlib.Path, pipeline: pathlib.Path) -> tp.Optional[str]:
    """
    Validate if the pipeline reference is present in the specified file.
    """
    error_message = None
    content = file_path.read_text().splitlines()
    if pipeline not in content:
        relative_file_path = file_path.relative_to(helpers.get_project_root())
        error_message = (
            f"The pipeline '{pipeline}' reference was not found in '{relative_file_path}'."
            f" \nKindly update the file {relative_file_path} to include a reference to the"
            f" newly added pipeline."
        )

    return error_message


def validate_pipeline_reference(pipelines: set[pathlib.Path]) -> None:
    errors = []

    for pipeline in pipelines:
        # 1. Validate if pipeline reference is present in CHANGELOG.md file
        # 2. Validate if pipeline reference is present in README.md file
        # 3. Validate if pipeline reference is present in docker-compose.yaml file
        # 4. Validate if pipeline reference is present in documentation/pipelines.md file
        for file in FILES_TO_VALIDATE:
            file_path = helpers.get_project_root() / file
            if error_msg := __validate_pipeline_reference_in_file(file_path, pipeline):
                errors.append(error_msg)

        # 5. Validate if pipeline reference is present in pipeline/README.md file
        pipeline_readme_path = helpers.get_project_root() / pipeline / helpers.README_FILE
        if not pipeline_readme_path.exists():
            relative_path = pipeline_readme_path.relative_to(helpers.get_project_root())
            raise ValidationError(
                f"Pipeline README file '{relative_path}' not found."
            )
        if error_msg := __validate_pipeline_reference_in_file(pipeline_readme_path, pipeline):
            errors.append(error_msg)

    # Raise error if any validation errors were found
    if errors:
        error_message = "\n\n".join(errors)
        raise ValidationError(
            f"Pipeline reference validation failed:\n{error_message}"
        )


def main():
    try:
        staged_pipelines = filter_pipeline_directory(helpers.get_all_modified_files())
        if not staged_pipelines:
            return 0

        current_pipelines = get_pipeline_files_from_origin_main()
        if new_pipelines := staged_pipelines - current_pipelines:
            validate_pipeline_reference(new_pipelines)

        return 0

    except subprocess.CalledProcessError as e:
        logger.error(f"Git command failed: {e}")
    except ValidationError as e:
        logger.error(f"Validation failed: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

    return 1


if __name__ == "__main__":
    sys.exit(main())
