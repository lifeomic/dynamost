module.exports = {
  preset: 'ts-jest',
  clearMocks: true,
  collectCoverage: true,
  coverageThreshold: {
    global: {
      statements: 72,
      branches: 46,
      functions: 66,
      lines: 71,
    },
  },
  collectCoverageFrom: [
    '<rootDir>/src/**/*.{ts,js}',
    '!<rootDir>/src/test/**/*',
  ],
};
