use inc::Module::Install;

name 'App-Cosmic';
all_from 'cosmic';
license 'bsd';
readme_from 'cosmic';

requires 'JSON';

install_script 'cosmic';

auto_include;
auto_install;

WriteAll;
