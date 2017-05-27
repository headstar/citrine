package com.tango.citrine;

import java.util.HashMap;
import java.util.Map;

/**
 * Map containing job specific data.
 *
 */
public class JobData extends HashMap<String, String> {

    private static final long serialVersionUID = -6679112704599738894L;

    public JobData() {
    }

    public JobData(Map<? extends String, ? extends String> m) {
        super(m);
    }

}
