module.exports = {
  preset: 'ts-jest',
  clearMocks: true,
  collectCoverage: true,
  coverageThreshold: {
    global: {
      statements: 68,
      branches: 40,
      functions: 63,
      lines: 67,
    },
  },
  collectCoverageFrom: [
    '<rootDir>/src/**/*.{ts,js}',
    '!<rootDir>/src/test/**/*',
  ],
};
