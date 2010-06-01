use inc::Module::Install;

name 'App-Cosmic';
all_from 'cosmic';
license 'bsd';
readme_from 'cosmic';

requires 'JSON' => 2.00;

install_script 'cosmic';
install_script 'cosmic-server';

auto_include;
auto_install;

auto_bundle_deps;

WriteAll;
