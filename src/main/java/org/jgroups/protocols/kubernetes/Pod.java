package org.jgroups.protocols.kubernetes;

import java.util.Objects;

public class Pod {

   private final String name;
   private final String podIp;
   private final String podGroup;    // name of group of Pods during Rolling Update. There is two groups: new pods and old pods
   private final boolean isReady;

   public Pod(String name, String podIp, String podGroup, boolean isReady) {
      this.name = name;
      this.podIp = podIp;
      this.podGroup = podGroup;
      this.isReady = isReady;
   }

   public String getName() {
      return name;
   }

   public String getPodIp() {
      return podIp;
   }

   public String getPodGroup() {
      return podGroup;
   }

   public boolean isReady() {
      return isReady;
   }

   @Override
   public String toString() {
      return "Pod{" +
            "name='" + name + '\'' +
            ", ip='" + podIp + '\'' +
            ", podGroup='" + podGroup + '\'' +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Pod pod = (Pod) o;

      if (!Objects.equals(name, pod.name)) return false;
      if (!Objects.equals(podIp, pod.podIp)) return false;
      return Objects.equals(podGroup, pod.podGroup);
   }

   @Override
   public int hashCode() {
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + (podIp != null ? podIp.hashCode() : 0);
      result = 31 * result + (podGroup != null ? podGroup.hashCode() : 0);
      return result;
   }
}
