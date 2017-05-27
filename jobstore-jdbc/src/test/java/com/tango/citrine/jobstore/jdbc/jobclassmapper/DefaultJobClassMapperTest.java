package com.tango.citrine.jobstore.jdbc.jobclassmapper;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.tango.citrine.Job;
import com.tango.citrine.JobExecutionContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by per on 14/04/15.
 */
public class DefaultJobClassMapperTest {


    @Test
    public void EmptyMapperToString() {
        // given
        JobClassMapper mapper = new DefaultJobClassMapper();

        // when
        String jobClassString = mapper.classToString(Foo1.class);

        // then
        assertEquals(Foo1.class.getName(), jobClassString);
    }

    @Test
    public void EmptyMapperToClass() throws ClassNotFoundException {
        // given
        JobClassMapper mapper = new DefaultJobClassMapper();

        // when
        Class<? extends Job> clazz = mapper.stringToClass(Foo1.class.getName());

        // then
        assertEquals(Foo1.class, clazz);
    }

    @Test
    public void CustomMappingToString() throws ClassNotFoundException {
        // given
        BiMap<Class<? extends Job>, String> mappings = HashBiMap.create();
        mappings.put(Foo1.class, "bar");
        JobClassMapper mapper = new DefaultJobClassMapper(mappings);

        // when
       String jobClassString = mapper.classToString(Foo1.class);

        // then
        assertEquals("bar", jobClassString);
    }

    @Test
    public void CustomMappingToClass() throws ClassNotFoundException {
        // given
        BiMap<Class<? extends Job>, String> mappings = HashBiMap.create();
        mappings.put(Foo1.class, "bar");
        JobClassMapper mapper = new DefaultJobClassMapper(mappings);

        // when
        Class<? extends Job> clazz = mapper.stringToClass("bar");

        // then
        assertEquals(Foo1.class, clazz);
    }

    @Test(expected = ClassNotFoundException.class)
    public void UnknownClassString() throws ClassNotFoundException {
        // given
        JobClassMapper mapper = new DefaultJobClassMapper();

        // when
        mapper.stringToClass("abc");

        // then ...exception should be thrown
    }

    public static class Foo1 implements Job {
        @Override
        public void execute(JobExecutionContext jobExecutionContext) {

        }
    }
}
