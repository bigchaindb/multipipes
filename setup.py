"""
multipipes: create and scale complex pipelines on multiple cores.
"""

# Heavily inspired by:
# https://github.com/bigchaindb/bigchaindb/blob/master/setup.py


from setuptools import setup, find_packages


tests_require = [
    'coverage',
    'pep8',
    'pyflakes',
    'pylint',
    'pytest',
    'pytest-cov',
    'pytest-xdist',
]

dev_require = [
    'ipdb',
    'ipython',
]

docs_require = [
    'recommonmark',
    'Sphinx',
    'sphinxcontrib-napoleon',
    'sphinx-rtd-theme',
]

setup(
    name='multipipes',
    version='0.1.0',
    description='Create and scale complex pipelines on multiple cores.',
    long_description=__doc__,
    url='https://github.com/bigchaindb/multipipes/',
    author='BigchainDB dev',
    author_email='alberto@bigchaindb.com',
    license='AGPLv3',
    zip_safe=False,

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development',
        'Natural Language :: English',
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',
    ],

    packages=find_packages(exclude=['tests*']),
    install_requires=[
    ],
    setup_requires=['pytest-runner'],
    tests_require=tests_require,
    extras_require={
        'test': tests_require,
        'dev': dev_require + tests_require + docs_require,
        'docs': docs_require,
    },
)

