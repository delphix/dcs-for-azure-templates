#
# Copyright (c) 2025 by Delphix. All rights reserved.
#

import logging
import pathlib
import typing as tp
import subprocess
import sys
import helpers

PIPELINE_DIR_PREFIX = "dcsazure_"
MASK_PIPELINE_DIR_SUFFIX = "_mask_pl"
DISCOVERY_PIPELINE_DIR_SUFFIX = "_discovery_pl"

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger("validate_new_pipeline_references")


class ValidationError(Exception):
    """Custom exception for validation-related errors."""


def get_pipeline_directory(path_files: tp.List[pathlib.Path]) -> set[pathlib.Path]:
    """
    Get the list of pipeline directories from the given list of filepath.
    """
    result = set()
    for path in path_files:
        pipeline_dir = path.parts[0]
        if (
                pipeline_dir.startswith(PIPELINE_DIR_PREFIX)
                and (
                pipeline_dir.endswith(MASK_PIPELINE_DIR_SUFFIX)
                or pipeline_dir.endswith(DISCOVERY_PIPELINE_DIR_SUFFIX))
        ):
            result.add(pathlib.Path(pipeline_dir))
    return result


def get_pipeline_files_from_origin_main() -> tp.List[pathlib.Path]:
    """
    Get the list of files in the current working directory
    """
    origin_main_files = helpers.get_files_from_origin_main()
    pipeline_files = get_pipeline_directory(origin_main_files)
    return list(pipeline_files)


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
    for pipeline in list(pipelines):
        # 1. Validate if pipeline reference is present in CHANGELOG.md file
        changelog_path = helpers.get_project_root() / helpers.CHANGELOG_FILE
        if error_msg := __validate_pipeline_reference_in_file(changelog_path, pipeline):
            errors.append(error_msg)

        # 2. Validate if pipeline reference is present in README.md file
        readme_path = helpers.get_project_root() / helpers.README_FILE
        if error_msg := __validate_pipeline_reference_in_file(readme_path, pipeline):
            errors.append(error_msg)

        # 3. Validate if pipeline reference is present in docker-compose.yaml file
        docker_compose_path = helpers.get_project_root() / helpers.DOCKER_COMPOSE_FILE
        if error_msg := __validate_pipeline_reference_in_file(docker_compose_path, pipeline):
            errors.append(error_msg)

        # 4. Validate if pipeline reference is present in documentation/pipelines.md file
        documentation_path = helpers.get_project_root() / helpers.DOCUMENTATION_FILE
        if error_msg := __validate_pipeline_reference_in_file(documentation_path, pipeline):
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
        staged_pipelines = list(get_pipeline_directory(helpers.get_all_modified_files()))
        if not staged_pipelines:
            return 0

        current_pipelines = get_pipeline_files_from_origin_main()
        if new_pipelines := set(staged_pipelines) - set(current_pipelines):
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
