import React from 'react';
import { render } from '@testing-library/react'

describe("a tsx test", () => {
    it("does react things", () => {
        const App = () => {
            return <h1>Hello, React with Rollup!</h1>;
        };

        render(<App />);
    })
})