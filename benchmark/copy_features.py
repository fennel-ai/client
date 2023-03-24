from datetime import datetime

from benchmark.social_network import UserViewsDataset, Request
from fennel.featuresets import featureset, feature, extractor, depends_on
from fennel.lib.metadata import meta
from fennel.lib.schema import Series


@meta(owner="feature-team@myspace.com")
@featureset
class UserCopyFeatures:
    num_views: int = feature(id=1)
    num_views_1: int = feature(id=2)
    num_views_2: int = feature(id=3)
    num_views_3: int = feature(id=4)
    num_views_4: int = feature(id=5)
    num_views_5: int = feature(id=6)
    num_views_6: int = feature(id=101)
    num_views_7: int = feature(id=7)
    num_views_8: int = feature(id=8)
    num_views_8: int = feature(id=9)
    num_views_9: int = feature(id=10)
    num_views_10: int = feature(id=11)
    num_views_11: int = feature(id=12)
    num_views_12: int = feature(id=13)
    num_views_13: int = feature(id=14)
    num_views_14: int = feature(id=15)
    num_views_15: int = feature(id=16)
    num_views_16: int = feature(id=17)
    num_views_17: int = feature(id=18)
    num_views_18: int = feature(id=19)
    num_views_19: int = feature(id=20)
    num_views_20: int = feature(id=21)
    num_views_21: int = feature(id=22)
    num_views_22: int = feature(id=23)
    num_views_23: int = feature(id=24)
    num_views_24: int = feature(id=25)
    num_views_25: int = feature(id=26)
    num_views_26: int = feature(id=27)
    num_views_27: int = feature(id=28)
    num_views_28: int = feature(id=29)
    num_views_29: int = feature(id=30)
    num_views_30: int = feature(id=31)
    num_views_31: int = feature(id=32)
    num_views_32: int = feature(id=33)
    num_views_33: int = feature(id=34)
    num_views_34: int = feature(id=35)
    num_views_35: int = feature(id=36)
    num_views_36: int = feature(id=37)
    num_views_37: int = feature(id=38)
    num_views_38: int = feature(id=39)
    num_views_39: int = feature(id=40)
    num_views_40: int = feature(id=41)
    num_views_41: int = feature(id=42)
    num_views_42: int = feature(id=43)
    num_views_43: int = feature(id=44)
    num_views_44: int = feature(id=45)
    num_views_45: int = feature(id=46)
    num_views_46: int = feature(id=47)
    num_views_47: int = feature(id=48)
    num_views_48: int = feature(id=49)
    num_views_49: int = feature(id=50)
    num_views_50: int = feature(id=51)
    num_views_51: int = feature(id=52)
    num_views_52: int = feature(id=53)
    num_views_53: int = feature(id=54)
    num_views_54: int = feature(id=55)
    num_views_55: int = feature(id=56)
    num_views_56: int = feature(id=57)
    num_views_57: int = feature(id=58)
    num_views_58: int = feature(id=59)
    num_views_59: int = feature(id=60)
    num_views_60: int = feature(id=61)
    num_views_61: int = feature(id=62)
    num_views_62: int = feature(id=63)
    num_views_63: int = feature(id=64)
    num_views_64: int = feature(id=65)
    num_views_65: int = feature(id=66)
    num_views_66: int = feature(id=67)
    num_views_67: int = feature(id=68)
    num_views_68: int = feature(id=69)
    num_views_69: int = feature(id=70)
    num_views_70: int = feature(id=71)
    num_views_71: int = feature(id=72)
    num_views_72: int = feature(id=73)
    num_views_73: int = feature(id=74)
    num_views_74: int = feature(id=75)
    num_views_75: int = feature(id=76)
    num_views_76: int = feature(id=77)
    num_views_77: int = feature(id=78)
    num_views_78: int = feature(id=79)
    num_views_79: int = feature(id=80)
    num_views_80: int = feature(id=81)
    num_views_81: int = feature(id=82)
    num_views_82: int = feature(id=83)
    num_views_83: int = feature(id=84)
    num_views_84: int = feature(id=85)
    num_views_85: int = feature(id=86)
    num_views_86: int = feature(id=87)
    num_views_87: int = feature(id=88)
    num_views_88: int = feature(id=89)
    num_views_89: int = feature(id=90)
    num_views_90: int = feature(id=91)
    num_views_91: int = feature(id=92)
    num_views_92: int = feature(id=93)
    num_views_93: int = feature(id=94)
    num_views_94: int = feature(id=95)
    num_views_95: int = feature(id=96)
    num_views_96: int = feature(id=97)
    num_views_97: int = feature(id=98)
    num_views_98: int = feature(id=99)
    num_views_99: int = feature(id=100)

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)  # type: ignore
        views = views.fillna(0)
        views["num_views"] = views["num_views"].astype(int)
        return views["num_views"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_1(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_1]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_1"] = views["num_views"].astype(int) * 1
        return views["num_views_1"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_2(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_2]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_2"] = views["num_views"].astype(int) * 2
        return views["num_views_2"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_3(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_3]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_3"] = views["num_views"].astype(int) * 3
        return views["num_views_3"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_4(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_4]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_4"] = views["num_views"].astype(int) * 4
        return views["num_views_4"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_5(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_5]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_5"] = views["num_views"].astype(int) * 5
        return views["num_views_5"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_6(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_6]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_6"] = views["num_views"].astype(int) * 6
        return views["num_views_6"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_7(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_7]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_7"] = views["num_views"].astype(int) * 7
        return views["num_views_7"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_8(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_8]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_8"] = views["num_views"].astype(int) * 8
        return views["num_views_8"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_9(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_9]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_9"] = views["num_views"].astype(int) * 9
        return views["num_views_9"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_10(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_10]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_10"] = views["num_views"].astype(int) * 10
        return views["num_views_10"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_11(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_11]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_11"] = views["num_views"].astype(int) * 11
        return views["num_views_11"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_12(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_12]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_12"] = views["num_views"].astype(int) * 12
        return views["num_views_12"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_13(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_13]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_13"] = views["num_views"].astype(int) * 13
        return views["num_views_13"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_14(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_14]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_14"] = views["num_views"].astype(int) * 14
        return views["num_views_14"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_15(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_15]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_15"] = views["num_views"].astype(int) * 15
        return views["num_views_15"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_16(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_16]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_16"] = views["num_views"].astype(int) * 16
        return views["num_views_16"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_17(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_17]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_17"] = views["num_views"].astype(int) * 17
        return views["num_views_17"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_18(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_18]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_18"] = views["num_views"].astype(int) * 18
        return views["num_views_18"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_19(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_19]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_19"] = views["num_views"].astype(int) * 19
        return views["num_views_19"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_20(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_20]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_20"] = views["num_views"].astype(int) * 20
        return views["num_views_20"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_21(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_21]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_21"] = views["num_views"].astype(int) * 21
        return views["num_views_21"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_22(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_22]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_22"] = views["num_views"].astype(int) * 22
        return views["num_views_22"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_23(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_23]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_23"] = views["num_views"].astype(int) * 23
        return views["num_views_23"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_24(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_24]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_24"] = views["num_views"].astype(int) * 24
        return views["num_views_24"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_25(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_25]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_25"] = views["num_views"].astype(int) * 25
        return views["num_views_25"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_26(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_26]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_26"] = views["num_views"].astype(int) * 26
        return views["num_views_26"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_27(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_27]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_27"] = views["num_views"].astype(int) * 27
        return views["num_views_27"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_28(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_28]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_28"] = views["num_views"].astype(int) * 28
        return views["num_views_28"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_29(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_29]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_29"] = views["num_views"].astype(int) * 29
        return views["num_views_29"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_30(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_30]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_30"] = views["num_views"].astype(int) * 30
        return views["num_views_30"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_31(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_31]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_31"] = views["num_views"].astype(int) * 31
        return views["num_views_31"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_32(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_32]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_32"] = views["num_views"].astype(int) * 32
        return views["num_views_32"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_33(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_33]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_33"] = views["num_views"].astype(int) * 33
        return views["num_views_33"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_34(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_34]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_34"] = views["num_views"].astype(int) * 34
        return views["num_views_34"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_35(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_35]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_35"] = views["num_views"].astype(int) * 35
        return views["num_views_35"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_36(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_36]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_36"] = views["num_views"].astype(int) * 36
        return views["num_views_36"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_37(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_37]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_37"] = views["num_views"].astype(int) * 37
        return views["num_views_37"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_38(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_38]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_38"] = views["num_views"].astype(int) * 38
        return views["num_views_38"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_39(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_39]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_39"] = views["num_views"].astype(int) * 39
        return views["num_views_39"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_40(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_40]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_40"] = views["num_views"].astype(int) * 40
        return views["num_views_40"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_41(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_41]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_41"] = views["num_views"].astype(int) * 41
        return views["num_views_41"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_42(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_42]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_42"] = views["num_views"].astype(int) * 42
        return views["num_views_42"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_43(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_43]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_43"] = views["num_views"].astype(int) * 43
        return views["num_views_43"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_44(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_44]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_44"] = views["num_views"].astype(int) * 44
        return views["num_views_44"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_45(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_45]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_45"] = views["num_views"].astype(int) * 45
        return views["num_views_45"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_46(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_46]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_46"] = views["num_views"].astype(int) * 46
        return views["num_views_46"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_47(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_47]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_47"] = views["num_views"].astype(int) * 47
        return views["num_views_47"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_48(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_48]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_48"] = views["num_views"].astype(int) * 48
        return views["num_views_48"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_49(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_49]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_49"] = views["num_views"].astype(int) * 49
        return views["num_views_49"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_50(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_50]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_50"] = views["num_views"].astype(int) * 50
        return views["num_views_50"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_51(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_51]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_51"] = views["num_views"].astype(int) * 51
        return views["num_views_51"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_52(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_52]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_52"] = views["num_views"].astype(int) * 52
        return views["num_views_52"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_53(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_53]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_53"] = views["num_views"].astype(int) * 53
        return views["num_views_53"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_54(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_54]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_54"] = views["num_views"].astype(int) * 54
        return views["num_views_54"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_55(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_55]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_55"] = views["num_views"].astype(int) * 55
        return views["num_views_55"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_56(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_56]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_56"] = views["num_views"].astype(int) * 56
        return views["num_views_56"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_57(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_57]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_57"] = views["num_views"].astype(int) * 57
        return views["num_views_57"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_58(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_58]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_58"] = views["num_views"].astype(int) * 58
        return views["num_views_58"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_59(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_59]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_59"] = views["num_views"].astype(int) * 59
        return views["num_views_59"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_60(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_60]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_60"] = views["num_views"].astype(int) * 60
        return views["num_views_60"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_61(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_61]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_61"] = views["num_views"].astype(int) * 61
        return views["num_views_61"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_62(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_62]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_62"] = views["num_views"].astype(int) * 62
        return views["num_views_62"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_63(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_63]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_63"] = views["num_views"].astype(int) * 63
        return views["num_views_63"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_64(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_64]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_64"] = views["num_views"].astype(int) * 64
        return views["num_views_64"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_65(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_65]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_65"] = views["num_views"].astype(int) * 65
        return views["num_views_65"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_66(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_66]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_66"] = views["num_views"].astype(int) * 66
        return views["num_views_66"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_67(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_67]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_67"] = views["num_views"].astype(int) * 67
        return views["num_views_67"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_68(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_68]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_68"] = views["num_views"].astype(int) * 68
        return views["num_views_68"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_69(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_69]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_69"] = views["num_views"].astype(int) * 69
        return views["num_views_69"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_70(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_70]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_70"] = views["num_views"].astype(int) * 70
        return views["num_views_70"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_71(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_71]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_71"] = views["num_views"].astype(int) * 71
        return views["num_views_71"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_72(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_72]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_72"] = views["num_views"].astype(int) * 72
        return views["num_views_72"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_73(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_73]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_73"] = views["num_views"].astype(int) * 73
        return views["num_views_73"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_74(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_74]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_74"] = views["num_views"].astype(int) * 74
        return views["num_views_74"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_75(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_75]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_75"] = views["num_views"].astype(int) * 75
        return views["num_views_75"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_76(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_76]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_76"] = views["num_views"].astype(int) * 76
        return views["num_views_76"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_77(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_77]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_77"] = views["num_views"].astype(int) * 77
        return views["num_views_77"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_78(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_78]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_78"] = views["num_views"].astype(int) * 78
        return views["num_views_78"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_79(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_79]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_79"] = views["num_views"].astype(int) * 79
        return views["num_views_79"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_80(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_80]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_80"] = views["num_views"].astype(int) * 80
        return views["num_views_80"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_81(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_81]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_81"] = views["num_views"].astype(int) * 81
        return views["num_views_81"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_82(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_82]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_82"] = views["num_views"].astype(int) * 82
        return views["num_views_82"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_83(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_83]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_83"] = views["num_views"].astype(int) * 83
        return views["num_views_83"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_84(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_84]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_84"] = views["num_views"].astype(int) * 84
        return views["num_views_84"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_85(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_85]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_85"] = views["num_views"].astype(int) * 85
        return views["num_views_85"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_86(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_86]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_86"] = views["num_views"].astype(int) * 86
        return views["num_views_86"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_87(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_87]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_87"] = views["num_views"].astype(int) * 87
        return views["num_views_87"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_88(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_88]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_88"] = views["num_views"].astype(int) * 88
        return views["num_views_88"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_89(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_89]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_89"] = views["num_views"].astype(int) * 89
        return views["num_views_89"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_90(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_90]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_90"] = views["num_views"].astype(int) * 90
        return views["num_views_90"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_91(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_91]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_91"] = views["num_views"].astype(int) * 91
        return views["num_views_91"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_92(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_92]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_92"] = views["num_views"].astype(int) * 92
        return views["num_views_92"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_93(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_93]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_93"] = views["num_views"].astype(int) * 93
        return views["num_views_93"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_94(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_94]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_94"] = views["num_views"].astype(int) * 94
        return views["num_views_94"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_95(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_95]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_95"] = views["num_views"].astype(int) * 95
        return views["num_views_95"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_96(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_96]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_96"] = views["num_views"].astype(int) * 96
        return views["num_views_96"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_97(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_97]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_97"] = views["num_views"].astype(int) * 97
        return views["num_views_97"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_98(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_98]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_98"] = views["num_views"].astype(int) * 98
        return views["num_views_98"]

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views_99(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views_99]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)
        views = views.fillna(0)
        views["num_views_99"] = views["num_views"].astype(int) * 99
        return views["num_views_99"]




