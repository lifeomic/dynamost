module.exports = {
  preset: 'ts-jest',
  clearMocks: true,
  collectCoverage: true,
  coverageThreshold: {
    global: {
      statements: 66,
      branches: 35,
      lines: 65,
      functions: 56,
    },
  },
};
