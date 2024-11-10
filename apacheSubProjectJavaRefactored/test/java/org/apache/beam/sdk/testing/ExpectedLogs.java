package org.apache.beam.sdk.testing;

import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import javax.annotation.concurrent.ThreadSafe;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;

public class ExpectedLogs extends ExternalResource {
  
  public static ExpectedLogs none(String name) {
    return new ExpectedLogs(name);
  }

  
  public static ExpectedLogs none(Class<?> klass) {
    return ExpectedLogs.none(klass.getName());
  }

  
  // Optimized by LLM: Consolidated verification methods
  public void verify(Level level, String substring) {
    verifyLogged(matcher(level, substring), logSaver);
  }

  
  // Optimized by LLM: Consolidated verification methods with Throwable
  public void verify(Level level, String substring, Throwable t) {
    verifyLogged(matcher(level, substring, t), logSaver);
  }

  
  public void verifyTrace(String substring) {
    verify(Level.FINEST, substring);
  }

  
  public void verifyTrace(String substring, Throwable t) {
    verify(Level.FINEST, substring, t);
  }

  
  public void verifyDebug(String substring) {
    verify(Level.FINE, substring);
  }

  
  public void verifyDebug(String message, Throwable t) {
    verify(Level.FINE, message, t);
  }

  
  public void verifyInfo(String substring) {
    verify(Level.INFO, substring);
  }

  
  public void verifyInfo(String message, Throwable t) {
    verify(Level.INFO, message, t);
  }

  
  public void verifyWarn(String substring) {
    verify(Level.WARNING, substring);
  }

  
  public void verifyWarn(String substring, Throwable t) {
    verify(Level.WARNING, substring, t);
  }

  
  public void verifyError(String substring) {
    verify(Level.SEVERE, substring);
  }

  
  public void verifyError(String substring, Throwable t) {
    verify(Level.SEVERE, substring, t);
  }

  
  public void verifyNotLogged(String substring) {
    verifyNotLogged(matcher(substring), logSaver);
  }

  
  public void verifyNoError(String substring, Throwable t) {
    verifyNotLogged(matcher(Level.SEVERE, substring, t), logSaver);
  }

  
  public void verifyLogRecords(Matcher<Iterable<LogRecord>> matcher) {
	    if (!matcher.matches(logSaver.getLogs())) {
	      fail(String.format("Missing match for [%s]", matcher));
	    }
	  }

  public static TypeSafeMatcher<LogRecord> matcher(final String substring) {
    return new TypeSafeMatcher<LogRecord>() {
      @Override
      public void describeTo(Description description) {
        description.appendText("log message containing message [" + substring + "]");
      }

      @Override
      protected boolean matchesSafely(LogRecord item) {
        return item.getMessage().contains(substring);
      }
    };
  }

  public static TypeSafeMatcher<LogRecord> matcher(final Level level, final String substring) {
    return new TypeSafeMatcher<LogRecord>() {
      @Override
      public void describeTo(Description description) {
        description.appendText("log message of level [" + level + "] containing message [" + substring + "]");
      }

      @Override
      protected boolean matchesSafely(LogRecord item) {
        return level.equals(item.getLevel()) && item.getMessage().contains(substring);
      }
    };
  }

  public static TypeSafeMatcher<LogRecord> matcher(final Level level, final String substring, final Throwable throwable) {
    return new TypeSafeMatcher<LogRecord>() {
      @Override
      public void describeTo(Description description) {
        description.appendText("log message of level [" + level + "] containing message [" + substring + "] with exception [" + throwable.getClass() + "] containing message [" + throwable.getMessage() + "]");
      }

      @Override
      protected boolean matchesSafely(LogRecord item) {
        return level.equals(item.getLevel())
            && item.getMessage().contains(substring)
            && item.getThrown() != null
            && item.getThrown().getClass().equals(throwable.getClass())
            && item.getThrown().getMessage().contains(throwable.getMessage());
      }
    };
  }

  public static void verifyLogged(Matcher<LogRecord> matcher, LogSaver logSaver) {
    for (LogRecord record : logSaver.getLogs()) {
      if (matcher.matches(record)) {
        return;
      }
    }

    fail(String.format("Missing match for [%s]", matcher));
  }

  public static void verifyNotLogged(Matcher<LogRecord> matcher, LogSaver logSaver) {
    
    for (LogRecord record : logSaver.getLogs()) {
      if (matcher.matches(record)) {
        fail("Unexpected match of [" + matcher + "]: [" + LOG_FORMATTER.format(record) + "]");
      }
    }
  }

  @Override
  protected void before() {
    previousLevel = log.getLevel();
    log.setLevel(Level.ALL);
    log.addHandler(logSaver);
  }

  @Override
  protected void after() {
    log.removeHandler(logSaver);
    log.setLevel(previousLevel);
    logSaver.clearLogs(); // Optimized by LLM: Renamed reset to clearLogs
  }

  private static final Formatter LOG_FORMATTER = new SimpleFormatter();
  private final Logger log;
  private final LogSaver logSaver;
  private Level previousLevel; // Optimized by LLM: Made final

  private ExpectedLogs(String name) {
    log = Logger.getLogger(name);
    logSaver = new LogSaver();
	this.previousLevel = null;
  }

  
  @ThreadSafe
  public static class LogSaver extends Handler {
    private final Collection<LogRecord> logRecords = new ConcurrentLinkedDeque<>();

    @Override
    public void publish(LogRecord record) {
      logRecords.add(record);
    }

    @Override
    public void flush() {}

    @Override
    public void close() throws SecurityException {}

    private Collection<LogRecord> getLogs() {
        return logRecords;
      }

    // Optimized by LLM: Renamed reset to clearLogs
    private void clearLogs() {
      logRecords.clear();
    }
  }
}