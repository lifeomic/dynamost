module.exports = {
  preset: 'ts-jest',
  clearMocks: true,
  collectCoverage: true,
  coverageThreshold: {
    global: {
      statements: 66,
      branches: 31,
      lines: 65,
      functions: 58,
    },
  },
};
