module.exports = {
  preset: 'ts-jest',
  clearMocks: true,
  collectCoverage: true,
  coverageThreshold: {
    global: {
      statements: 66,
      branches: 36,
      lines: 66,
      functions: 56,
    },
  },
};
