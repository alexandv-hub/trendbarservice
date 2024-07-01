# Trend Bar Service

## Terminology

- **Symbol**: Currency pair (e.g., EURUSD, EURJPY).
- **Quote**: A price update for a particular symbol which occurred at a certain moment. Contains new price, symbol, and timestamp.
- **Trend Bar (TB)**: Also known as a candlestick, it is aggregated quote data for a particular symbol within a given trend bar period. It contains the following parameters:
    - **Open Price**: Price for a given symbol at the beginning of a trend bar period.
    - **Close Price**: Price from the last received quote in this trend bar period.
    - **High Price**: Maximum value of the price during the period.
    - **Low Price**: Minimum value of the price during the period.
    - **Trend Bar Period**: Certain time interval during which quotes are accumulated (e.g., M1 - one minute, H1 - one hour, D1 - one day, etc.).
    - **Timestamp**: Moment of time at which the trend bar period starts.
- **Completed TB**: A TB for which the time interval is over. For example, if considering TB of type M1, then completed TBs are those created before or at the beginning of the previous minute.
- **TB History**: A set of completed TBs whose start time falls within a specific period.

## General Requirements

- Sources must be compilable. If they are not, the task is not considered completed.
- Source code must be covered with tests. JUnit or TestNG can be used as test frameworks.
- It's crucial to make the source code as clean and easy to understand as possible, adhering to the principles outlined in the "Clean Code" book. Good code is supposed to document itself, so don't waste time on Javadoc.
- Code should be formatted according to standard Java Code Style.
- Project structure should be Maven-compliant and have a `pom.xml` file in the root directory that describes the project and its dependencies.

## Task Description

Assume there are three types of trend bars: M1 (minutely), H1 (hourly), and D1 (daily).

Implement a trend bar service that:

1. **Builds TBs based on received quotes**: The service should maintain a set of current trend bars, update them with each quote, and persist them to some storage as soon as the period for a trend bar is over.
2. **Provides trend bars history upon request**: It should return a set of trend bars for a given symbol name, trend bar period, and time range (from timestamp to timestamp). If the `to` timestamp is omitted, the method should return all trend bars from the `from` timestamp to now.

## Limitations and Clarifications

- Keep it simple.
- Don’t use third-party frameworks except for dependency injection (Spring, Guice) and mocking (Mockito, JMock) frameworks.
- Use Dependency Injection where applicable.
- Use Test Driven Development to implement this task for clean and concise code.
- TB history includes completed TBs only.
- TB storage can be in-memory.
- Quotes arrive in one thread, and history requests are made in another.
- Quotes arrive in natural order, with each subsequent quote having a larger timestamp than the previous one.
- The number of quotes received per minute can be very high.
- Trend bar service is a module of a system, not a standalone application, so there is no need to expose this service via web services or implement a UI interface for it.

## Bonuses

- The TB service should process received quotes asynchronously, meaning it should put received quotes into some kind of internal storage for further processing by another thread.
- It would be beneficial to have trivial implementations of quote providers for use in integration tests.