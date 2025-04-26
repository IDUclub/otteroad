from setuptools import setup, find_packages

setup(
    name="idu-kafka-client",
    version="0.1.0",
    description="Framework with scalable Kafka consumer/producer logic for IDU FastAPI services.",
    author="Ruslan Babayev",
    author_email="rus.babaef@yandex.ru",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=("tests*", "examples*", "scripts*")),
    python_requires=">=3.11,<4.0",
    install_requires=[
        "pydantic>=2.11.3,<3.0.0",
        "confluent-kafka>=2.9.0,<3.0.0",
        "pyyaml>=6.0.2,<7.0.0",
        "python-dotenv>=1.1.0,<2.0.0",
        "fastavro>=1.10.0,<2.0.0",
    ],
    classifiers=[
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries",
    ],
    include_package_data=True,
    zip_safe=False,
)
