package io.github.mananmonga.bullseye.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.WildcardType;
import java.lang.reflect.GenericArrayType;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * Guards design decision D9: nothing under {@code io.github.mananmonga.bullseye.internal} may appear
 * in a public signature of this module's public package. The package name and {@code @Internal}
 * are documentation; this is enforcement. It is the only design decision a well-meaning change can
 * violate while compiling and passing every other test, and it would surface at the Flink 2.x
 * transport swap, years later.
 */
class ApiSurfaceTest {

    private static final String PUBLIC_PACKAGE = "io.github.mananmonga.bullseye.kafka";
    private static final String INTERNAL_PREFIX = "io.github.mananmonga.bullseye.internal";

    @Test
    void noInternalTypeInAnyPublicSignature() throws Exception {
        List<Class<?>> publicTypes = publicTypesIn(PUBLIC_PACKAGE);
        assertThat(publicTypes).as("scanned public types").isNotEmpty();
        Set<String> leaks = new TreeSet<>();
        for (Class<?> c : publicTypes) {
            for (Type t : c.getGenericInterfaces()) {
                check(leaks, c + " implements", t);
            }
            check(leaks, c + " extends", c.getGenericSuperclass());
            for (TypeVariable<?> tv : c.getTypeParameters()) {
                for (Type b : tv.getBounds()) {
                    check(leaks, c + " <" + tv + ">", b);
                }
            }
            for (Field f : c.getDeclaredFields()) {
                if (isExported(f.getModifiers())) {
                    check(leaks, c.getSimpleName() + "." + f.getName(), f.getGenericType());
                }
            }
            for (Constructor<?> k : c.getDeclaredConstructors()) {
                if (isExported(k.getModifiers())) {
                    for (Type p : k.getGenericParameterTypes()) {
                        check(leaks, c.getSimpleName() + ".<init>", p);
                    }
                }
            }
            for (Method m : c.getDeclaredMethods()) {
                if (!isExported(m.getModifiers()) || m.isSynthetic()) {
                    continue;
                }
                String where = c.getSimpleName() + "." + m.getName();
                check(leaks, where, m.getGenericReturnType());
                for (Type p : m.getGenericParameterTypes()) {
                    check(leaks, where, p);
                }
                for (Type e : m.getGenericExceptionTypes()) {
                    check(leaks, where, e);
                }
            }
        }
        assertThat(leaks).as("internal types leaking into the public API").isEmpty();
    }

    private static boolean isExported(int modifiers) {
        return Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers);
    }

    private static void check(Set<String> leaks, String where, Type t) {
        if (t == null) {
            return;
        }
        if (t instanceof Class) {
            Class<?> c = (Class<?>) t;
            while (c.isArray()) {
                c = c.getComponentType();
            }
            if (c.getName().startsWith(INTERNAL_PREFIX)) {
                leaks.add(where + " -> " + c.getName());
            }
        } else if (t instanceof ParameterizedType) {
            ParameterizedType p = (ParameterizedType) t;
            check(leaks, where, p.getRawType());
            for (Type a : p.getActualTypeArguments()) {
                check(leaks, where, a);
            }
        } else if (t instanceof WildcardType) {
            for (Type b : ((WildcardType) t).getUpperBounds()) {
                check(leaks, where, b);
            }
            for (Type b : ((WildcardType) t).getLowerBounds()) {
                check(leaks, where, b);
            }
        } else if (t instanceof GenericArrayType) {
            check(leaks, where, ((GenericArrayType) t).getGenericComponentType());
        } else if (t instanceof TypeVariable) {
            for (Type b : ((TypeVariable<?>) t).getBounds()) {
                check(leaks, where, b);
            }
        }
    }

    /** Public top-level and nested types declared directly in {@code pkg} (not sub-packages). */
    private static List<Class<?>> publicTypesIn(String pkg) throws Exception {
        List<Class<?>> out = new ArrayList<>();
        String path = pkg.replace('.', '/');
        java.util.Enumeration<URL> urls = ApiSurfaceTest.class.getClassLoader().getResources(path);
        while (urls.hasMoreElements()) {
            URL url = urls.nextElement();
            if (!"file".equals(url.getProtocol())) {
                continue;
            }
            File dir = new File(url.toURI());
            if (dir.getPath().contains("test")) {
                continue; // only the main classes directory
            }
            File[] files = dir.listFiles((d, name) -> name.endsWith(".class"));
            if (files == null) {
                continue;
            }
            for (File f : files) {
                String name = f.getName().substring(0, f.getName().length() - ".class".length());
                Class<?> c = Class.forName(pkg + "." + name);
                if (Modifier.isPublic(c.getModifiers())) {
                    out.add(c);
                }
            }
        }
        return out;
    }
}
