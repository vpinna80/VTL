import setuptools

setuptools.setup(
    name="${project.name}",
    version="${python.package.version}",
    author="Valentino Pinna",
    author_email="valentino.pinna@bancaditalia.it",
    description="${project.description}",
    long_description="${project.description}",
    long_description_content_type="text/plain",
    url="${site-url}",
    packages=setuptools.find_packages(),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: European Union Public Licence 1.2 (EUPL 1.2)",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        "Jpype1", "ipykernel", "numpy", "pandas"
    ]
)