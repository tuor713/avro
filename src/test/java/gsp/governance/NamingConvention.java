package gsp.governance;

import org.apache.avro.Schema;
import org.junit.Test;
import static org.junit.Assert.*;

import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Governance test to check for naming convention adherence:
 * - Field and record names are capital case
 * - Only a single variant of capitalization for all fields with the same lower case name
 */
public class NamingConvention {

    private static Set<Schema.Type> AVRO_PRIMITIVE_TYPES = new HashSet<Schema.Type>(
            Arrays.asList(
                    Schema.Type.BOOLEAN,
                    Schema.Type.BYTES,
                    Schema.Type.DOUBLE,
                    Schema.Type.FLOAT,
                    Schema.Type.INT,
                    Schema.Type.LONG,
                    Schema.Type.NULL,
                    Schema.Type.STRING
            )
    );

    private boolean isSimpleSchema(Schema schema) {
        boolean primitiveType = AVRO_PRIMITIVE_TYPES.contains(schema.getType());
        boolean unionOfPrimitives = schema.getType() == Schema.Type.UNION && schema.getTypes().stream().allMatch(t -> AVRO_PRIMITIVE_TYPES.contains(t.getType()));
        return primitiveType || unionOfPrimitives;
    }

    private Collection<Path> allSchemaFiles() throws Exception {
        List<Path> result = new ArrayList<>();

        Files.walkFileTree(Paths.get("src/main/avro"),
                new SimpleFileVisitor<Path>() {
                    public FileVisitResult visitFile(Path path, BasicFileAttributes attr) {
                        result.add(path);
                        return FileVisitResult.CONTINUE;
                    }
                });

        return result;
    }

    private Stream<Schema> allSchemas() throws Exception {
        Schema.Parser parser = new Schema.Parser();

        return allSchemaFiles().stream()
                .map(
                        path -> {
                            try {
                                return parser.parse(Files.newInputStream(path));
                            } catch (Exception e) {
                                e.printStackTrace();
                                return null;
                            }
                        }
                )
                .filter(Objects::nonNull);
    }

    private Stream<Schema.Field> allFields(Stream<Schema> schemas) {
        return schemas.flatMap(this::allFields);
    }

    private Stream<Schema.Field> allFields(Schema schema) {
        if (schema.getType() == Schema.Type.RECORD) {
            return Stream.concat(schema.getFields().stream(), schema.getFields().stream().flatMap(f -> allFields(f.schema())));
        } else if (schema.getType() == Schema.Type.UNION) {
            return schema.getTypes().stream().flatMap(this::allFields);
        } else {
            return Stream.empty();
        }
    }

    private Stream<Schema> allRecords(Stream<Schema> schemas) {
        return schemas.flatMap(this::allRecords);
    }

    private Stream<Schema> allRecords(Schema schema) {
        if (schema.getType() == Schema.Type.RECORD) {
            return Stream.concat(Stream.of(schema), schema.getFields().stream().flatMap(f -> allRecords(f.schema())));
        } else if (schema.getType() == Schema.Type.UNION) {
            return schema.getTypes().stream().flatMap(this::allRecords);
        } else {
            return Stream.empty();
        }
    }

    @Test
    public void testFieldsCapitalized() throws Exception {
        allFields(allSchemas()).forEach(
                f -> {
                    System.out.println("Checking field name " + f.name());
                    assertTrue("Field name must be capitalized: "+f.name(), Character.isUpperCase(f.name().charAt(0)));
                }
        );

        Map<String, List<String>> fs = allFields(allSchemas()).map(Schema.Field::name).distinct().collect(Collectors.groupingBy(String::toLowerCase));

        fs.entrySet().stream().forEach(
                e -> {
                    System.out.println("Checking consistent naming " + e.getKey());
                    assertEquals(1, e.getValue().size());
                }
        );
    }
}
