import { execSync } from 'child_process';
import { unlinkSync } from 'fs';
import glob from 'glob';

const run = (cmd: string) =>
  execSync(cmd, { cwd: __dirname, stdio: 'inherit' });

run('rm -rf dist/');

run('yarn tsc');

for (const file of ['package.json', 'README.md']) {
  run(`cp ${file} dist/`);
}

// Remove test files from output
for (const file of glob.sync('dist/**/*.test.*')) {
  unlinkSync(file);
}

console.log('✔️  Successfully built library to dist folder');
