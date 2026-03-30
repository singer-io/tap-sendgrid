from setuptools import setup, find_packages

setup(
    name="tap-sendgrid",
    version="2.0.0",
    description="Singer.io tap for extracting data from the SendGrid v3 API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    install_requires=[
        "singer-python==6.1.1",
        "requests==2.31.0",
        "backoff==2.2.1",
    ],
    extras_require={
        "dev": [
            "pytest",
            "coverage",
        ]
    },
    entry_points="""
        [console_scripts]
        tap-sendgrid=tap_sendgrid:main
    """,
    packages=find_packages(),
    package_data={
        "tap_sendgrid": ["schemas/*.json"],
    },
    include_package_data=True,
)
