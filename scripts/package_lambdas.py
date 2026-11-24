from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAMBDAS_DIR = PROJECT_ROOT / "lambdas"
BUILD_DIR = PROJECT_ROOT / "build"

BUILD_DIR.mkdir(parents=True, exist_ok=True)

generator_source = LAMBDAS_DIR / "data_generator" / "lambda_function.py"

generator_zip = BUILD_DIR / "coldchain_data_generator.zip"

with ZipFile(generator_zip, "w", ZIP_DEFLATED) as zip_file:
    zip_file.write(generator_source, arcname="lambda_function.py")

def package_consumer(consumer_name, zip_name):
    consumer_dir = LAMBDAS_DIR / consumer_name
    zip_path = BUILD_DIR / zip_name

    source_files = [
        LAMBDAS_DIR / "__init__.py",
        LAMBDAS_DIR / "common" / "__init__.py",
        LAMBDAS_DIR / "common" / "sensor_processor.py",
        consumer_dir / "lambda_function.py"
    ]

    if (consumer_dir / "__init__.py").exists():
        source_files.append(consumer_dir / "__init__.py")

    with ZipFile(zip_path, "w", ZIP_DEFLATED) as zip_file:
        for source_file in source_files:
            relative_path = source_file.relative_to(PROJECT_ROOT)
            zip_file.write(source_file, arcname=relative_path)

    print(f"Created deployment package: {zip_path.name}")

package_consumer(
    "event_processor",
    "coldchain_event_processor.zip"
)

package_consumer(
    "long_poll_consumer",
    "coldchain_long_poll_consumer.zip"
)