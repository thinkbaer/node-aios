import * as glob from 'glob';
[
  ...glob.sync('node_modules/*/*/gulp/*.js'),
  ...glob.sync('node_modules/*/gulp/*.js'),
  ...glob.sync('src/gulp/*'),
  ...glob.sync('gulp/*'),
]
  .filter(x => !/@types\//.test(x))
  .map(x => require('./' + x));
