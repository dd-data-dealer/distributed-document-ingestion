# Troubleshooting

Known issues encountered while building and testing the PDF pipeline.

## 1. Java package unavailable during Docker build

**Error:**

```text
E: Package 'openjdk-17-jre-headless' has no installation candidate
```

Docker build exited with code `100`.

**Cause:** The Docker base image did not provide the expected Java 17 package.

**Fix:** Pin the Python base image to Debian Bookworm:

```dockerfile
FROM python:3.11-slim-bookworm
```

and install Java 17 explicitly.

**Status:** Resolved

---

## 2. PySpark / Java incompatibility at runtime

**Error:**

```text
java.lang.UnsupportedOperationException:
sun.misc.Unsafe or java.nio.DirectByteBuffer.<init>(long, int) not available
```

**Cause:** The Java runtime installed through `default-jre-headless` was incompatible with the PySpark environment.

**Fix:** Use a pinned Debian Bookworm image with Java 17 instead of `default-jre-headless`.

For Apple Silicon:

```dockerfile
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
```

**Status:** Resolved

---

## Build vs. Runtime Issues

**`docker build`**  
↓  
**ERROR #1 — Build-time dependency problem**  
**Type:** OS / package dependency  
**Issue:** Java package unavailable  
↓  
**Status:** ✓ Resolved  
↓  
**Docker image builds successfully**  
↓  
**`docker run`**  
↓  
**ERROR #2 — Runtime compatibility problem**  
**Type:** Spark ↔ Java compatibility  
↓  
**Status:** ✓ Resolved  
↓  
**Spark pipeline runs successfully**
---

## 3. PDF parsing — `io` not defined

**Error:**

```text
PARSE_ERROR: name 'io' is not defined
```

**Cause:** `io.BytesIO()` was used without importing Python's `io` module.

**Fix:**

```python
import io
```

**Status:** Resolved


## 4. Duplicate rows in parse_partition()
**Cause:**  yield was inside the row loop, emitting accumulated results repeatedly.

**Fix:** Move yield pd.DataFrame(results) outside the inner loop.

**Status:** Resolved

