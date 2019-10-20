from setuptools import setup


setup(name='cian_bot',
      author='galinova@sports.ru',
      packages=['cian_parser'],
      package_dir={'': 'src'},
      install_requires=['requests', 'pyjsparser', 'beautifulsoup4'])
