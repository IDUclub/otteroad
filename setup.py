from setuptools import setup, find_packages

setup(
    name="otteroad",
    version="0.2.2",
    description="Framework with scalable Kafka consumer/producer logic for IDU FastAPI services.",
    author="Ruslan Babayev",
    author_email="rus.babaef@yandex.ru",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    license="Apache-2.0",
    packages=find_packages(exclude=("tests*", "examples*", "scripts*")),
    python_requires=">=3.10,<4.0",
    install_requires=[
        "pydantic>=2.11.3,<3.0.0",
        "pyyaml>=6.0.2,<7.0.0",
        "python-dotenv>=1.1.0,<2.0.0",
        "fastavro>=1.10.0,<2.0.0",
        "confluent-kafka[schemaregistry]>=2.10.0,<3.0.0",
    ],
    classifiers=[
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries",
    ],
    include_package_data=True,
    zip_safe=False,
)
